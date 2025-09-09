package pancakeswap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/sujine/market-fetcher/exchange"
	pb "github.com/sujine/market-fetcher/proto"
	"github.com/sujine/market-fetcher/publisher"
)

/*
ENV (optional)
- BSC_WS_URL                : WS RPC (default: wss://bsc-rpc.publicnode.com)
- BSC_HTTP_URL              : HTTP RPC for fallback polling (e.g., https://bsc-dataseed.binance.org/)
- DEX_POOL_ADDRS            : CSV pool addresses (global) — per symbol overrides: DEX_POOL_ADDRS_ETHUSDT
- DEX_CONFIRMATIONS         : Number of confirmations for safe blocks (default 3)
- DEX_LOG_POLL_MS           : HTTP log polling interval in ms (default 2000)
- DEX_TWAP_POLL_MS          : TWAP polling interval in ms (default 5000)
- DEX_TWAP_WINDOW_SEC       : TWAP window in seconds for observe() (default 60)
- DEX_STARTUP_SCAN_BLOCKS   : If > 0, performs a one-time FilterLogs for the last N blocks (for debugging)
*/

const (
	// Standard Multicall3 contract address, same across many EVM chains
	MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
	PANCAKE_V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"

	DEFAULT_BSC_WS_URL = "wss://bsc-rpc.publicnode.com"
	DEFAULT_BSC_HTTP   = "https://bsc-dataseed.binance.org" // A public HTTP endpoint for fallback

	// Pancake v3 "Swap" event signature hash
	swapEventSigHex = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"

	// Common token addresses on BSC
	WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
	USDT = "0x55d398326f99059fF775485246999027B3197955"
	WETH = "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
)

// Fee tiers to check when auto-discovering pools via factory
var feeTiers = []uint32{100, 500, 2500, 10000}


type Collector struct {
	Symbol string
	Pools  []common.Address
	Pub    publisher.Nats

	ws         *ethclient.Client
	http       *ethclient.Client // Optional client for HTTP polling fallback
	poolABI    abi.ABI
	factoryABI abi.ABI // ABI for the PancakeV3 Factory
	multiABI   abi.ABI // ABI for the Multicall3 contract

	mu         sync.RWMutex
	lastBlock  uint64
	lastIdx    uint
	lastSwapAt int64

	// Token decimal information, crucial for correct price calculation
	token0         common.Address
	token1         common.Address
	token0Decimals int
	token1Decimals int

	invertForQuote bool

	confirmations uint64
	logPollEvery  time.Duration
	twapWindow    uint32
	twapEvery     time.Duration
}

func New(symbol string, pools []common.Address, pub publisher.Nats) *Collector {
	return &Collector{
		Symbol: strings.ToUpper(symbol),
		Pools:  pools,
		Pub:    pub,
	}
}

func (c *Collector) Name() string  { return "PancakeSwapV3(" + c.Symbol + ")" }
func (c *Collector) Venue() string { return "dex" }

func (c *Collector) Run(ctx context.Context) error {
	// Setup configuration from environment variables or use defaults
	wsURL := env("BSC_WS_URL", DEFAULT_BSC_WS_URL)
	httpURL := env("BSC_HTTP_URL", DEFAULT_BSC_HTTP)
	c.confirmations = uint64(envInt("DEX_CONFIRMATIONS", 3))
	c.logPollEvery = envDurationMS("DEX_LOG_POLL_MS", 2000)
	c.twapWindow = uint32(envInt("DEX_TWAP_WINDOW_SEC", 60))
	c.twapEvery = envDurationMS("DEX_TWAP_POLL_MS", 5000)

	// Establish WebSocket connection
	if err := c.connectWS(ctx, wsURL); err != nil {
		return err
	}
	defer c.ws.Close()

	// Establish optional HTTP connection for fallback
	if httpURL != "" {
		hc, err := ethclient.DialContext(ctx, httpURL)
		if err == nil {
			c.http = hc
			log.Printf("[%s] HTTP fallback enabled: %s", c.Name(), httpURL)
			defer c.http.Close()
		} else {
			log.Printf("[%s] WARN: HTTP fallback disabled (dial failed): %v", c.Name(), err)
		}
	}

	// Load ABIs
	var err error
	if c.poolABI, err = abi.JSON(strings.NewReader(poolABIJSON)); err != nil {
		return fmt.Errorf("load pool ABI: %w", err)
	}
	if c.factoryABI, err = abi.JSON(strings.NewReader(factoryABIJSON)); err != nil {
		return fmt.Errorf("load factory ABI: %w", err)
	}
	if c.multiABI, err = abi.JSON(strings.NewReader(multicallABIJSON)); err != nil {
		return fmt.Errorf("load multicall ABI: %w", err)
	}

	// Get token info (addresses, decimals) for the symbol
	if err := c.setupTokenInfo(); err != nil {
		return err
	}

	// Discover and verify pools from environment or factory contract
	c.discoverAndVerifyPools(ctx)
	if len(c.Pools) == 0 {
		return fmt.Errorf("no valid pools found for %s (check ENV or factory)", c.Symbol)
	}
	log.Printf("[%s] Monitoring %d pool(s)", c.Name(), len(c.Pools))
	for _, p := range c.Pools {
		log.Printf("[%s] -> Pool Address: %s", c.Name(), p.Hex())
	}

	// (Optional) Scan recent blocks on startup for debugging
	if scan := envInt("DEX_STARTUP_SCAN_BLOCKS", 0); scan > 0 {
		if err := c.debugScanRecent(ctx, uint64(scan)); err != nil {
			log.Printf("[%s] recent-scan error: %v", c.Name(), err)
		}
	}

	// Start background goroutines
	// go c.pollTwap(ctx)
	for _, p := range c.Pools {
		go c.subscribeSwapWS(ctx, p)
		// if c.http != nil {
		// 	go c.pollLogsFallback(ctx, p)
		// }
	}
	
	<-ctx.Done()
	return ctx.Err()
}

func (c *Collector) publishSwap(mid float64, tsNs int64) {
	evt := publisher.NewMarketDataBuilder(
		"PancakeSwapV3", c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithTick(0, 0, mid, 0, 0, 0, 0).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.TsNs = tsNs

	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishTwap(spot, twap float64, tsNs int64) {
	evt := publisher.NewMarketDataBuilder(
		"PancakeSwapV3", c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithTick(0, 0, spot, 0, 0, twap, 0).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.TsNs = tsNs

	_ = c.Pub.Publish(evt)
}


// setupTokenInfo resolves and stores token addresses and decimals for the collector's symbol.
func (c *Collector) setupTokenInfo() error {
	t0, t1, d0, d1, ok := getTokenInfoForSymbol(c.Symbol)
	if !ok {
		return fmt.Errorf("unsupported symbol: %s", c.Symbol)
	}
	// Pancake/Uni v3 규칙: token0 < token1 (주소 오름차순)
    // bytes.Compare(a,b) < 0  => a < b
    if bytes.Compare(t0.Bytes(), t1.Bytes()) <= 0 {
        // token0=t0(base), token1=t1(quote)
        c.token0, c.token1 = t0, t1
        c.token0Decimals, c.token1Decimals = d0, d1
        c.invertForQuote = false // 풀 가격(token1/token0)이 곧 quote/base → 역수 불필요
    } else {
        // token0=t1(quote), token1=t0(base)
        c.token0, c.token1 = t1, t0
        c.token0Decimals, c.token1Decimals = d1, d0
        c.invertForQuote = true // 풀 가격(token1/token0)=base/quote → 우리가 원하는 quote/base는 역수
    }
	log.Printf("[%s] Configured for %s: token0=%s (%d dec), token1=%s (%d dec)",
		c.Name(), c.Symbol, c.token0.Hex(), c.token0Decimals, c.token1.Hex(), c.token1Decimals)
	return nil
}

// connectWS establishes a persistent WebSocket connection with backoff retries.
func (c *Collector) connectWS(ctx context.Context, wsURL string) error {
	log.Printf("[%s] Connecting to WebSocket: %s", c.Name(), wsURL)
	var err error
	for backoff := 200 * time.Millisecond; ; backoff *= 2 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c.ws, err = ethclient.DialContext(ctx, wsURL)
		if err == nil {
			log.Printf("[%s] WebSocket connected", c.Name())
			return nil
		}
		if backoff > 10*time.Second {
			return fmt.Errorf("WS dial failed after multiple retries: %w", err)
		}
		log.Printf("[%s] WS dial error: %v. Retrying in %v...", c.Name(), err, backoff)
		time.Sleep(backoff)
	}
}

// subscribeSwapWS subscribes to Swap events for a single pool.
func (c *Collector) subscribeSwapWS(ctx context.Context, pool common.Address) {
	sig := common.HexToHash(swapEventSigHex)
	ch := make(chan types.Log, 256)
	q := ethereum.FilterQuery{Addresses: []common.Address{pool}, Topics: [][]common.Hash{{sig}}}

	for {
		if ctx.Err() != nil {
			return
		}
		sub, err := c.ws.SubscribeFilterLogs(ctx, q, ch)
		if err != nil {
			log.Printf("[%s] WS sub error pool=%s: %v (retrying)", c.Name(), pool.Hex(), err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("[%s] WS subscribed to pool=%s", c.Name(), pool.Hex())

	RETRY:
		for {
			select {
			case err := <-sub.Err():
				log.Printf("[%s] WS subscription lost for pool=%s: %v. Re-subscribing...", c.Name(), pool.Hex(), err)
				sub.Unsubscribe()
				time.Sleep(1 * time.Second)
				break RETRY
			case v, ok := <-ch:
				if !ok {
					log.Printf("[%s] log channel closed by provider (re-subscribing)", c.Name())
					sub.Unsubscribe()
					time.Sleep(1 * time.Second)
					break RETRY
				}
				log.Printf("[%s] recv log blk=%d idx=%d", c.Name(), v.BlockNumber, v.Index)
				c.onSwapLog(v)
			case <-time.After(30 * time.Second):
				log.Printf("[%s] still subscribed to %s, waiting for Swap logs...", c.Name(), pool.Hex())
			
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			}
		}
	}
}

// pollLogsFallback provides a polling mechanism over HTTP as a backup to WS.
func (c *Collector) pollLogsFallback(ctx context.Context, pool common.Address) {
	sig := common.HexToHash(swapEventSigHex)
	q := ethereum.FilterQuery{Addresses: []common.Address{pool}, Topics: [][]common.Hash{{sig}}}

	t := time.NewTicker(c.logPollEvery)
	defer t.Stop()

	var fromBlk uint64
	for {
		select {
		case <-t.C:
			latest, err := c.http.BlockNumber(ctx)
			if err != nil {
				continue
			}
			safe := latest
			if c.confirmations > 0 && latest > c.confirmations {
				safe = latest - c.confirmations
			}

			c.mu.RLock()
			// If we haven't seen a block yet, start from the latest safe block.
			// Otherwise, start from the last seen block.
			start := c.lastBlock
			if start == 0 {
				start = safe
			}
			c.mu.RUnlock()

			// Ensure we don't re-poll blocks we've already polled
			if fromBlk > 0 && start < fromBlk {
				start = fromBlk
			}
			if start > safe {
				continue
			}

			q.FromBlock = new(big.Int).SetUint64(start)
			q.ToBlock = new(big.Int).SetUint64(safe)

			logs, err := c.http.FilterLogs(ctx, q)
			if err != nil {
				continue
			}
			for _, lg := range logs {
				c.onSwapLog(lg)
			}
			fromBlk = safe + 1 // Next poll should start from here

		case <-ctx.Done():
			return
		}
	}
}

// onSwapLog processes a single Swap event log.
func (c *Collector) onSwapLog(vLog types.Log) {
    if vLog.Removed {
        log.Printf("[%s] Skipping removed log: blk=%d idx=%d", c.Name(), vLog.BlockNumber, vLog.Index)
        return
    }

    // 이벤트 디코딩 (이벤트 전용 API)
    swapEv, ok := c.poolABI.Events["Swap"]
    if !ok {
        log.Printf("[%s] ABI has no Swap event", c.Name())
        return
    }
    nonIdx, err := swapEv.Inputs.NonIndexed().Unpack(vLog.Data)
    if err != nil {
        log.Printf("[%s] Swap non-indexed unpack failed pool=%s blk=%d idx=%d err=%v",
            c.Name(), vLog.Address.Hex(), vLog.BlockNumber, vLog.Index, err)
        return
    }
    if len(nonIdx) < 5 {
        log.Printf("[%s] Swap unpack insufficient fields", c.Name())
        return
    }

    sqrtPriceX96, _ := nonIdx[2].(*big.Int)
    if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
        log.Printf("[%s] sqrtPriceX96 invalid", c.Name())
        return
    }

    // 중복/오래된 로그 스킵
    c.mu.Lock()
    if (vLog.BlockNumber < c.lastBlock) || (vLog.BlockNumber == c.lastBlock && vLog.Index <= c.lastIdx) {
        c.mu.Unlock()
        return
    }
    c.lastBlock = vLog.BlockNumber
    c.lastIdx = vLog.Index
    c.lastSwapAt = time.Now().UnixNano()
    c.mu.Unlock()

    mid := c.sqrtPriceX96ToPrice(sqrtPriceX96)
	tsNs := time.Now().UnixNano()

	c.publishSwap(mid, tsNs)

    log.Printf("[%s] SWAP published mid=%.6f pool=%s blk=%d", c.Name(), mid, vLog.Address.Hex(), vLog.BlockNumber)
}


type mcCall struct {
	Target       common.Address `abi:"target"`
	AllowFailure bool           `abi:"allowFailure"`
	CallData     []byte         `abi:"callData"`
}
type mcResult struct {
	Success    bool   `abi:"success"`
	ReturnData []byte `abi:"returnData"`
}

// pollTwap periodically fetches spot and TWAP prices using a single multicall and publishes the result.
func (c *Collector) pollTwap(ctx context.Context) {
	if len(c.Pools) == 0 {
		return
	}
	pool := c.Pools[0]
	t := time.NewTicker(c.twapEvery)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			// 최근 스왑이 한동안 없으면 observe 호출 스킵 (불필요한 RPC 절감)
			// c.mu.RLock()
			// lastSwapAt := c.lastSwapAt
			// c.mu.RUnlock()
			// if lastSwapAt == 0 || time.Since(time.Unix(0, lastSwapAt)) > 2*c.twapEvery {
			// 	continue
			// }

			// --- 1) 멀티콜 경로 ---
			spotPrice, twapPrice, _ := func() (float64, float64, bool) {
				// calldata 준비
				slot0Data, err := c.poolABI.Pack("slot0")
				if err != nil {
					return 0, 0, false
				}
				secondsAgos := []uint32{c.twapWindow, 0}
				observeData, err := c.poolABI.Pack("observe", secondsAgos)
				if err != nil {
					return 0, 0, false
				}

				calls := []mcCall{
					{Target: pool, AllowFailure: true, CallData: slot0Data},
					{Target: pool, AllowFailure: true, CallData: observeData},
				}
				mcData, err := c.multiABI.Pack("aggregate3", calls)
				if err != nil {
					return 0, 0, false
				}

				mcAddr := common.HexToAddress(MULTICALL3_ADDRESS)
				res, err := c.ws.CallContract(ctx, ethereum.CallMsg{To: &mcAddr, Data: mcData}, nil)
				if err != nil || len(res) == 0 {
					return 0, 0, false
				}

				out, err := c.multiABI.Unpack("aggregate3", res)
				if err != nil || len(out) == 0 {
					return 0, 0, false
				}

				// 결과 캐스팅: []mcResult 로 안전 변환
				var results []mcResult
				switch v := out[0].(type) {
				case []struct {
					Success    bool
					ReturnData []byte
				}:
					results = make([]mcResult, len(v))
					for i := range v {
						results[i] = mcResult{Success: v[i].Success, ReturnData: v[i].ReturnData}
					}
				case []mcResult:
					results = v
				default:
					// go-ethereum 리플렉션 차이 방어
					conv, ok := abi.ConvertType(out[0], new([]mcResult)).(*[]mcResult)
					if !ok || conv == nil {
						return 0, 0, false
					}
					results = *conv
				}
				
				if len(results) != 2 {
					return 0, 0, false
				}

				// slot0 → spot
				var sqrtPrice *big.Int
				if results[0].Success {
					slot0Out, err := c.poolABI.Unpack("slot0", results[0].ReturnData)
					if err == nil && len(slot0Out) > 0 {
						sqrtPrice, _ = slot0Out[0].(*big.Int)
					}
				}
				if sqrtPrice == nil || sqrtPrice.Sign() == 0 {
					return 0, 0, false
				}
				spot := c.sqrtPriceX96ToPrice(sqrtPrice)

				// observe → twap
				var twap float64
				if results[1].Success {
					obsOut, err := c.poolABI.Unpack("observe", results[1].ReturnData)
					if err == nil && len(obsOut) > 0 {
						if ticks, ok := obsOut[0].([]*big.Int); ok && len(ticks) == 2 && c.twapWindow > 0 {
							diff := new(big.Int).Sub(ticks[1], ticks[0])
							avgTick := new(big.Int).Div(diff, big.NewInt(int64(c.twapWindow))).Int64()
							twap = c.tickToPrice(avgTick)
						}
					}
				}
				return spot, twap, true
			}()

			if spotPrice <= 0 {
				continue
			}

			tsNs := time.Now().UnixNano()
			c.publishTwap(spotPrice, twapPrice, tsNs)
			log.Printf("[%s] TWAP/SPOT published spot=%.6f twap=%.6f pool=%s", c.Name(), spotPrice, twapPrice, pool.Hex())

		case <-ctx.Done():
			return
		}
	}
}

// fetchSlot0AndObserveMultiCall uses Multicall3 to fetch data from two functions in a single RPC call.
func (c *Collector) fetchSlot0AndObserveMultiCall(ctx context.Context, pool common.Address) (*big.Int, []*big.Int, error) {
	// 1. Prepare calldata for individual calls
	slot0Data, err := c.poolABI.Pack("slot0")
	if err != nil {
		return nil, nil, fmt.Errorf("pack slot0 failed: %w", err)
	}
	secondsAgos := []uint32{c.twapWindow, 0}
	observeData, err := c.poolABI.Pack("observe", secondsAgos)
	if err != nil {
		return nil, nil, fmt.Errorf("pack observe failed: %w", err)
	}

	// 2. Construct the multicall payload
	type Call3 struct {
		Target       common.Address
		AllowFailure bool
		CallData     []byte
	}
	calls := []Call3{
		{Target: pool, AllowFailure: true, CallData: slot0Data},
		{Target: pool, AllowFailure: true, CallData: observeData},
	}

	multiCallData, err := c.multiABI.Pack("aggregate3", calls)
	if err != nil {
		return nil, nil, fmt.Errorf("pack aggregate3 failed: %w", err)
	}

	// 3. Make the single multicall
	multiCallAddr := common.HexToAddress(MULTICALL3_ADDRESS)
	res, err := c.ws.CallContract(ctx, ethereum.CallMsg{To: &multiCallAddr, Data: multiCallData}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("multicall failed: %w", err)
	}

	// 4. Unpack the multicall result. Using Unpack for older go-ethereum compatibility.
	unpackedResults, err := c.multiABI.Unpack("aggregate3", res)
	if err != nil {
		return nil, nil, fmt.Errorf("unpack aggregate3 failed: %w", err)
	}
	if len(unpackedResults) == 0 {
		return nil, nil, errors.New("multicall returned empty result set")
	}

	// Cast the result to the expected struct slice type
	results, ok := unpackedResults[0].([]struct {
		Success    bool
		ReturnData []byte
	})
	if !ok {
		return nil, nil, errors.New("failed to cast multicall results")
	}
	if len(results) != 2 {
		return nil, nil, errors.New("multicall returned incorrect number of results")
	}

	// 5. Unpack the individual results
	var sqrtPrice *big.Int
	if results[0].Success {
		slot0Result, err := c.poolABI.Unpack("slot0", results[0].ReturnData)
		if err == nil && len(slot0Result) > 0 {
			sqrtPrice, _ = slot0Result[0].(*big.Int)
		}
	}

	var tickCumulatives []*big.Int
	if results[1].Success {
		observeResult, err := c.poolABI.Unpack("observe", results[1].ReturnData)
		if err == nil && len(observeResult) > 0 {
			tickCumulatives, _ = observeResult[0].([]*big.Int)
		}
	}

	if sqrtPrice == nil || sqrtPrice.Sign() == 0 {
		return nil, nil, errors.New("failed to get valid sqrtPrice from multicall")
	}

	return sqrtPrice, tickCumulatives, nil
}

// discoverAndVerifyPools finds pools from ENV or by querying the factory contract.
func (c *Collector) discoverAndVerifyPools(ctx context.Context) {
	if len(c.Pools) > 0 {
		return // Pools were provided manually
	}
	if env := envPoolsForSymbol(c.Symbol); len(env) > 0 {
		c.Pools = env
		log.Printf("[%s] Found %d pool(s) via environment variable", c.Name(), len(env))
	} else {
		log.Printf("[%s] No pools in ENV, auto-discovering via factory...", c.Name())
		autoPools := c.discoverPoolsViaFactory(ctx)
		if len(autoPools) > 0 {
			c.Pools = autoPools
			log.Printf("[%s] Auto-discovered %d pool(s)", c.Name(), len(autoPools))
		}
	}
}

// discoverPoolsViaFactory finds pool addresses for the collector's token pair and fee tiers.
func (c *Collector) discoverPoolsViaFactory(ctx context.Context) []common.Address {
	factory := common.HexToAddress(PANCAKE_V3_FACTORY)
	var discoveredPools []common.Address

	tokenA, tokenB := c.token0, c.token1

	for _, fee := range feeTiers {
		// Pack calldata for the getPool function
		callData, err := c.factoryABI.Pack("getPool", tokenA, tokenB, new(big.Int).SetUint64(uint64(fee)))
		if err != nil {
			log.Printf("[%s] Failed to pack getPool for fee %d: %v", c.Name(), fee, err)
			continue
		}

		// Call the factory contract
		msg := ethereum.CallMsg{To: &factory, Data: callData}
		res, err := c.ws.CallContract(ctx, msg, nil)
		if err != nil {
			log.Printf("[%s] getPool call failed for fee %d: %v", c.Name(), fee, err)
			continue
		}

		// Unpack the result
		unpacked, err := c.factoryABI.Unpack("getPool", res)
		if err != nil || len(unpacked) == 0 {
			continue
		}

		poolAddr, ok := unpacked[0].(common.Address)
		// The factory returns address(0) if the pool doesn't exist
		if ok && poolAddr != (common.Address{}) {
			discoveredPools = append(discoveredPools, poolAddr)
		}
	}
	return discoveredPools
}

// --- Helper Functions ---

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}
func envDurationMS(key string, defMS int) time.Duration {
	return time.Duration(envInt(key, defMS)) * time.Millisecond
}
func envPoolsForSymbol(sym string) []common.Address {
	key := "DEX_POOL_ADDRS_" + strings.ToUpper(strings.TrimSpace(sym))
	if addrs := splitCSVAddrs(os.Getenv(key)); len(addrs) > 0 {
		return addrs
	}
	return splitCSVAddrs(os.Getenv("DEX_POOL_ADDRS"))
}
func splitCSVAddrs(s string) []common.Address {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var out []common.Address
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if common.IsHexAddress(p) {
			out = append(out, common.HexToAddress(p))
		}
	}
	return out
}

// getTokenInfoForSymbol returns token addresses and decimals for a given symbol.
// This should be expanded for other pairs.
func getTokenInfoForSymbol(sym string) (t0, t1 common.Address, d0, d1 int, ok bool) {
	s := strings.ToUpper(strings.TrimSpace(sym))
	switch s {
	case "ETHUSDT":
		return common.HexToAddress(WETH), common.HexToAddress(USDT), 18, 18, true
	case "WBNBUSDT", "BNBUSDT":
		return common.HexToAddress(WBNB), common.HexToAddress(USDT), 18, 18, true
	default:
		return common.Address{}, common.Address{}, 0, 0, false
	}
}

// sqrtPriceX96ToPrice converts slot0.sqrtPriceX96 into a normal price.
// - Pool rule: price01 = token1/token0
// - Adjusts for decimals (dec0 - dec1)
// - If symbol wants quote/base but pool order is reversed, invert with c.invertForQuote
func (c *Collector) sqrtPriceX96ToPrice(sqrtPriceX96 *big.Int) float64 {
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
		return 0
	}
	// Formula: price = (sqrtPriceX96 / 2^96)^2 * 10^(decimals0 - decimals1)
	num := new(big.Float).SetInt(sqrtPriceX96)
	q96 := new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 96))

	ratio := new(big.Float).Quo(num, q96)
	p := new(big.Float).Mul(ratio, ratio)

	// Adjust for token decimals
	decimalAdjustment := math.Pow10(c.token0Decimals - c.token1Decimals)
	p.Mul(p, new(big.Float).SetFloat64(decimalAdjustment))

	p01, _ := p.Float64() // token1/token0
    if p01 <= 0 {
        return 0
    }

	if c.invertForQuote {
        return 1.0 / p01
    }
	return p01
}

// tickToPrice calculates price from a V3 tick, adjusting for decimals.
func (c *Collector) tickToPrice(tick int64) float64 {
	// Formula: price = 1.0001^tick * 10^(decimals0 - decimals1)
	price := math.Pow(1.0001, float64(tick))
	decimalAdjustment := math.Pow10(c.token0Decimals - c.token1Decimals)
	return price * decimalAdjustment
}

func (c *Collector) currentSafeBlock() (uint64, error) {
	client := c.ws
	if c.http != nil { // Prefer http for block number as it's less stateful
		client = c.http
	}
	if client == nil {
		return 0, errors.New("no active client")
	}
	latest, err := client.BlockNumber(context.Background())
	if err != nil {
		return 0, err
	}
	if c.confirmations == 0 || latest <= c.confirmations {
		return latest, nil
	}
	return latest - c.confirmations, nil
}

func (c *Collector) debugScanRecent(ctx context.Context, lastN uint64) error {
	if len(c.Pools) == 0 {
		return nil
	}
	latest, err := c.ws.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("could not get blockNumber: %w", err)
	}
	start := uint64(0)
	if latest > lastN {
		start = latest - lastN
	}
	q := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(start),
		ToBlock:   new(big.Int).SetUint64(latest),
		Addresses: c.Pools,
		Topics:    [][]common.Hash{{common.HexToHash(swapEventSigHex)}},
	}
	logs, err := c.ws.FilterLogs(ctx, q)
	if err != nil {
		return err
	}
	log.Printf("[%s] Recent-scan found %d swap logs in last %d blocks", c.Name(), len(logs), lastN)
	return nil
}


// --- Registry ---

func init() {
	exchange.Register("pancakeswapv3", func(symbol string, pub publisher.Nats) exchange.Collector {
		return New(symbol, nil, pub)
	})
}

