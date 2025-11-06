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

// TxReplayer listens to logs on a source chain and replays the original
// transactions to a local fork (hardhat/anvil) by impersonating senders.
type TxReplayer struct {
	// source chain
	srcWS   *ethclient.Client
	srcHTTP *ethclient.Client
	srcRPC  *rpc.Client

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

// observedLog holds ordering info for a single log.
type observedLog struct {
	TxHash      common.Hash
	BlockNumber uint64
	TxIndex     uint
	LogIndex    uint
	BlockTime   time.Time
	Addr        common.Address
}

// New builds a replayer from env.
// REPLAYER_TARGETS="0xabc,0xdef" is required.
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

// Run connects to source + fork and starts subscription + replay.
func (r *TxReplayer) Run(ctx context.Context, forkURL string) error {
	// source endpoints (generic first, then old BSC names)
	srcWSURL := firstNonEmpty(
		os.Getenv("REPLAYER_MAIN_WS"),
		os.Getenv("SRC_WS_URL"),
		os.Getenv("ETH_WS_URL"),
		os.Getenv("BSC_WS_URL"),
	)
	if srcWSURL == "" {
		return fmt.Errorf("source WS not set (set REPLAYER_MAIN_WS or BSC_WS_URL)")
	}

	srcHTTPURL := firstNonEmpty(
		os.Getenv("REPLAYER_MAIN_HTTP"),
		os.Getenv("SRC_HTTP_URL"),
		os.Getenv("ETH_HTTP_URL"),
		os.Getenv("BSC_HTTP_URL"),
	)
	if srcHTTPURL == "" {
		return fmt.Errorf("source HTTP not set (set REPLAYER_MAIN_HTTP or BSC_HTTP_URL)")
	}

	var err error
	// source HTTP/RPC
	r.srcRPC, err = rpc.DialContext(ctx, srcHTTPURL)
	if err != nil {
		return fmt.Errorf("dial source http rpc: %w", err)
	}
	r.srcHTTP = ethclient.NewClient(r.srcRPC)

	// source WS
	r.srcWS, err = ethclient.DialContext(ctx, srcWSURL)
	if err != nil {
		return fmt.Errorf("dial source ws: %w", err)
	}

	// fork endpoint (if not provided explicitly)
	if forkURL == "" {
		forkURL = getenv("FORK_RPC_URL", "http://127.0.0.1:8545")
	}

	// fork RPC/HTTP
	r.forkRPC, err = rpc.DialContext(ctx, forkURL)
	if err != nil {
		return fmt.Errorf("dial fork rpc: %w", err)
	}
	r.forkHTTP = ethclient.NewClient(r.forkRPC)

	// start workers
	go r.replayLoop(ctx)
	go r.subscribeTargets(ctx)

	<-ctx.Done()
	return ctx.Err()
}

// subscribeTargets subscribes logs from all target addresses on source chain.
func (r *TxReplayer) subscribeTargets(ctx context.Context) {
	ch := make(chan types.Log, 256)
	q := ethereum.FilterQuery{
		Addresses: r.targets,
	}
	for {
		sub, err := r.srcWS.SubscribeFilterLogs(ctx, q, ch)
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
				// fetch on-chain timestamp for ordering
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

// replayLoop orders events and replays transactions to fork.
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

			// order by (block, tx, log)
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
				// try to keep original time gap
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

// replayOne fetches original tx from source and sends same call to fork.
func (r *TxReplayer) replayOne(ctx context.Context, h common.Hash) error {
	// fetch original tx
	tx, _, err := r.srcHTTP.TransactionByHash(ctx, h)
	if err != nil {
		return fmt.Errorf("get source tx: %w", err)
	}

	// recover sender from source chain
	chainID, err := r.srcHTTP.NetworkID(ctx)
	if err != nil {
		return fmt.Errorf("source network id: %w", err)
	}
	signer := types.LatestSignerForChainID(chainID)
	from, err := types.Sender(signer, tx)
	if err != nil {
		return fmt.Errorf("decode sender: %w", err)
	}

	// impersonate sender on fork
	if err := r.impersonate(ctx, from); err != nil {
		return fmt.Errorf("impersonate: %w", err)
	}

	// ALWAYS fund from local rich account (not setBalance)
	if err := r.fundAccount(ctx, from); err != nil {
		// not fatal, but log it
		log.Printf("[replayer] fund warn for %s: %v", from.Hex(), err)
	}

	// get nonce on fork for that sender
	nonce, err := r.forkHTTP.PendingNonceAt(ctx, from)
	if err != nil {
		return fmt.Errorf("fork nonce: %w", err)
	}

	// get gas price
	gp, err := r.forkHTTP.SuggestGasPrice(ctx)
	if err != nil {
		gp = big.NewInt(1_000_000_000)
	}

	to := tx.To()
	if to == nil {
		// contract creation, skip for now
		return fmt.Errorf("tx is contract creation, skip")
	}

	callArgs := map[string]interface{}{
		"from":     from.Hex(),
		"to":       to.Hex(),
		"gas":      hexutil.Uint64(tx.Gas()),
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

// fundAccount sends some ETH/BNB from a local rich account to the target EOA.
// This does NOT use hardhat_setBalance / anvil_setBalance to avoid "missing trie node" on pruned RPCs.
func (r *TxReplayer) fundAccount(ctx context.Context, target common.Address) error {
	// rich account is the local pre-funded account that the fork exposes
	rich := getenv("FORK_RICH_ACCOUNT", "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	richAddr := common.HexToAddress(rich)

	// get nonce for rich account on fork
	nonce, err := r.forkHTTP.PendingNonceAt(ctx, richAddr)
	if err != nil {
		return fmt.Errorf("fallback nonce from rich: %w", err)
	}

	// gas price
	gp, err := r.forkHTTP.SuggestGasPrice(ctx)
	if err != nil {
		gp = big.NewInt(1_000_000_000)
	}

	// send 1 ETH/BNB equivalent
	txArgs := map[string]interface{}{
		"from":     richAddr.Hex(),
		"to":       target.Hex(),
		"value":    hexutil.EncodeBig(big.NewInt(1e18)), // 1 * 10^18
		"gas":      hexutil.Uint64(21000),
		"gasPrice": hexutil.Big(*gp),
		"nonce":    hexutil.Uint64(nonce),
	}

	var txHash common.Hash
	if err := r.forkRPC.CallContext(ctx, &txHash, "eth_sendTransaction", txArgs); err != nil {
		return fmt.Errorf("rich transfer failed: %w", err)
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

// fetchBlockTime fetches block timestamp on source chain.
func (r *TxReplayer) fetchBlockTime(ctx context.Context, num uint64) time.Time {
	blk, err := r.srcHTTP.BlockByNumber(ctx, big.NewInt(int64(num)))
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

// ---------- helpers ----------

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
