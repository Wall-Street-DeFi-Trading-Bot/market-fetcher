package replayer

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// TxReplayer is a generic "mainnet-to-fork" transaction replayer.
// It listens to logs from given target addresses on mainnet (WS),
// collects tx hashes in chain order, and re-sends the original tx
// to a local fork (hardhat/anvil) by impersonating the original sender.
type TxReplayer struct {
	// mainnet / source
	mainWS   *ethclient.Client
	mainHTTP *ethclient.Client
	mainRPC  *rpc.Client

	// fork / destination
	forkHTTP *ethclient.Client
	forkRPC  *rpc.Client

	// target contract addresses (pools, routers, ...)
	targets []common.Address

	// buffered observed logs
	evCh chan observedLog

	// max sleep between replays to prevent too slow tests
	MaxSleep time.Duration
}

// observedLog is a single log observed on mainnet.
type observedLog struct {
	TxHash      common.Hash
	BlockNumber uint64
	TxIndex     uint
	LogIndex    uint
	BlockTime   time.Time
	Addr        common.Address
}

// New creates a new replayer.
// It reads targets from env REPLAYER_TARGETS="0xabc,..."
func New() (*TxReplayer, error) {
	tgts, err := parseTargets(os.Getenv("REPLAYER_TARGETS"))
	if err != nil {
		return nil, err
	}
	if len(tgts) == 0 {
		return nil, fmt.Errorf("no targets: set REPLAYER_TARGETS=0x...,0x...")
	}

	r := &TxReplayer{
		targets:  tgts,
		evCh:     make(chan observedLog, 1024),
		MaxSleep: 5 * time.Second,
	}
	return r, nil
}

// Run connects to mainnet + fork and starts both subscription and replay loop.
func (r *TxReplayer) Run(ctx context.Context) error {
	// source (mainnet/bsc) endpoints
	mainWSURL := getenv("REPLAYER_MAIN_WS", getenv("BSC_WS_URL", "wss://bsc-ws-node.nariox.org:443"))
	mainHTTPURL := getenv("REPLAYER_MAIN_HTTP", getenv("BSC_HTTP_URL", "https://bsc-dataseed.binance.org"))

	// fork endpoint
	forkURL := getenv("FORK_RPC_URL", "http://127.0.0.1:8545")

	var err error
	r.mainRPC, err = rpc.DialContext(ctx, mainHTTPURL)
	if err != nil {
		return fmt.Errorf("dial main http rpc: %w", err)
	}
	r.mainHTTP = ethclient.NewClient(r.mainRPC)

	r.mainWS, err = ethclient.DialContext(ctx, mainWSURL)
	if err != nil {
		return fmt.Errorf("dial main ws: %w", err)
	}

	r.forkRPC, err = rpc.DialContext(ctx, forkURL)
	if err != nil {
		return fmt.Errorf("dial fork rpc: %w", err)
	}
	r.forkHTTP = ethclient.NewClient(r.forkRPC)

	// start replay worker
	go r.replayLoop(ctx)

	// start single subscription for all targets
	go r.subscribeTargets(ctx)

	<-ctx.Done()
	return ctx.Err()
}

// subscribeTargets subscribes logs from all target addresses.
func (r *TxReplayer) subscribeTargets(ctx context.Context) {
	ch := make(chan types.Log, 256)
	q := ethereum.FilterQuery{
		Addresses: r.targets,
		// no topics -> any event from those addresses
	}
	for {
		sub, err := r.mainWS.SubscribeFilterLogs(ctx, q, ch)
		if err != nil {
			log.Printf("[replayer] subscribe error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for {
			select {
			case <-sub.Err():
				sub.Unsubscribe()
				time.Sleep(1 * time.Second)
				goto RETRY
			case lg := <-ch:
				if lg.Removed {
					continue
				}
				// fetch block time once here
				ts := r.fetchBlockTime(ctx, lg.BlockNumber)
				r.evCh <- observedLog{
					TxHash:      lg.TxHash,
					BlockNumber: uint64(lg.BlockNumber),
					TxIndex:     uint(lg.TxIndex),
					LogIndex:    uint(lg.Index),
					BlockTime:   ts,
					Addr:        lg.Address,
				}
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			}
		}
	RETRY:
	}
}

// replayLoop drains observed logs, sorts them by chain order, and replays the original txs.
func (r *TxReplayer) replayLoop(ctx context.Context) {
	buf := make([]observedLog, 0, 256)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var lastTime time.Time

	for {
		select {
		case ev := <-r.evCh:
			buf = append(buf, ev)

		case <-ticker.C:
			if len(buf) == 0 {
				continue
			}

			// sort by (block, tx, log)
			sort.Slice(buf, func(i, j int) bool {
				if buf[i].BlockNumber != buf[j].BlockNumber {
					return buf[i].BlockNumber < buf[j].BlockNumber
				}
				if buf[i].TxIndex != buf[j].TxIndex {
					return buf[i].TxIndex < buf[j].TxIndex
				}
				return buf[i].LogIndex < buf[j].LogIndex
			})

			for _, ev := range buf {
				// keep time gap roughly
				if !lastTime.IsZero() && !ev.BlockTime.IsZero() {
					delta := ev.BlockTime.Sub(lastTime)
					if delta > 0 {
						if delta > r.MaxSleep {
							delta = r.MaxSleep
						}
						select {
						case <-time.After(delta):
						case <-ctx.Done():
							return
						}
					}
				}

				if err := r.replayOne(ctx, ev.TxHash); err != nil {
					log.Printf("[replayer] replay %s failed: %v", ev.TxHash.Hex(), err)
				} else {
					log.Printf("[replayer] replayed %s", ev.TxHash.Hex())
				}

				if !ev.BlockTime.IsZero() {
					lastTime = ev.BlockTime
				}
			}

			buf = buf[:0]

		case <-ctx.Done():
			return
		}
	}
}

// replayOne fetches the original tx from mainnet and re-sends it to fork by impersonating the sender.
func (r *TxReplayer) replayOne(ctx context.Context, h common.Hash) error {
	// 1) get original tx
	tx, _, err := r.mainHTTP.TransactionByHash(ctx, h)
	if err != nil {
		return fmt.Errorf("get main tx: %w", err)
	}

	// 2) find sender from mainnet chainID
	chainID, err := r.mainHTTP.NetworkID(ctx)
	if err != nil {
		return fmt.Errorf("main network id: %w", err)
	}
	signer := types.LatestSignerForChainID(chainID)
	from, err := types.Sender(signer, tx)
	if err != nil {
		return fmt.Errorf("decode sender: %w", err)
	}

	// 3) impersonate on fork (hardhat/anvil)
	if err := r.impersonate(ctx, from); err != nil {
		return fmt.Errorf("impersonate: %w", err)
	}

	// 4) top up balance for this account
	if err := r.setBalance(ctx, from, big.NewInt(0).Mul(big.NewInt(1e18), big.NewInt(100))); err != nil {
		// not fatal
		log.Printf("[replayer] setBalance warn: %v", err)
	}

	// 5) get fork nonce
	nonce, err := r.forkHTTP.PendingNonceAt(ctx, from)
	if err != nil {
		return fmt.Errorf("fork nonce: %w", err)
	}

	// 6) gas price to use on fork
	gp, err := r.forkHTTP.SuggestGasPrice(ctx)
	if err != nil {
		gp = big.NewInt(1_000_000_000) // 1 gwei fallback
	}

	to := tx.To()
	if to == nil {
		// contract creation tx: skip
		return fmt.Errorf("tx is contract creation, skip")
	}

    callArgs := map[string]interface{}{
        "from":     from.Hex(),
        "to":       to.Hex(),
        "gas":      hexutil.Uint64(tx.Gas()),
        // hexutil.Big expects a value type, not *big.Int
        "gasPrice": hexutil.Big(*gp),
        "value":    hexutil.Big(*tx.Value()),
        "data":     hexutil.Encode(tx.Data()),
        "nonce":    hexutil.Uint64(nonce),
    }

	var result common.Hash
	if err := r.forkRPC.CallContext(ctx, &result, "eth_sendTransaction", callArgs); err != nil {
		return fmt.Errorf("fork send: %w", err)
	}

	return nil
}

// impersonate unlocks account on fork.
func (r *TxReplayer) impersonate(ctx context.Context, addr common.Address) error {
	var out interface{}
	// try hardhat
	if err := r.forkRPC.CallContext(ctx, &out, "hardhat_impersonateAccount", addr.Hex()); err != nil {
		// try anvil
		if err2 := r.forkRPC.CallContext(ctx, &out, "anvil_impersonateAccount", addr.Hex()); err2 != nil {
			return err
		}
	}
	return nil
}

// setBalance funds account on fork.
func (r *TxReplayer) setBalance(ctx context.Context, addr common.Address, wei *big.Int) error {
	var out interface{}
	hexBal := hexutil.EncodeBig(wei)
	if err := r.forkRPC.CallContext(ctx, &out, "hardhat_setBalance", addr.Hex(), hexBal); err != nil {
		if err2 := r.forkRPC.CallContext(ctx, &out, "anvil_setBalance", addr.Hex(), hexBal); err2 != nil {
			return err
		}
	}
	return nil
}

// fetchBlockTime fetches block timestamp on mainnet.
func (r *TxReplayer) fetchBlockTime(ctx context.Context, num uint64) time.Time {
	blk, err := r.mainHTTP.BlockByNumber(ctx, big.NewInt(int64(num)))
	if err != nil || blk == nil {
		return time.Time{}
	}
	return time.Unix(int64(blk.Time()), 0)
}

// parseTargets parses comma-separated addresses.
func parseTargets(s string) ([]common.Address, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	parts := strings.Split(s, ",")
	out := make([]common.Address, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !common.IsHexAddress(p) {
			return nil, fmt.Errorf("invalid address: %s", p)
		}
		out = append(out, common.HexToAddress(p))
	}
	return out, nil
}

// getenv returns env or default.
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
