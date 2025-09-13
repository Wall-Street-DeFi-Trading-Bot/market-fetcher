// Package: pancakeswap
// File: pancakeswap_unified_v2_v3.go
//
// Unified collector for PancakeSwap v2+v3 with ZERO duplicated flows.
// - One subscribe function (subscribePool)
// - One log handler (handleLog)
// - ENV per symbol: DEX_POOLS_<SYMBOL>=v3@0x...,v2@0x...,0x...(raw->v3)
// - v3 falls back to factory discovery if no v3 in ENV
// - v2 uses Swap event (NOT Sync) and computes trade price; optionally you can still listen to Sync elsewhere
// - Both versions publish DexSwapL1 (v2 uses sqrtPriceX96=0, tick=0, liquidity=0)
//
// Notes:
// - Keep ABIs minimal and local for portability; you can split them back to abi.go if you prefer.

package pancakeswap

import (
	"bytes"
	"context"
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

// --------- Constants & defaults ---------

const (
	PANCAKE_V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
	DEFAULT_BSC_WS_URL = "wss://bsc-rpc.publicnode.com"
	DEFAULT_BSC_HTTP   = "https://bsc-dataseed.binance.org"

	// Common tokens on BSC
	WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
	USDT = "0x55d398326f99059fF775485246999027B3197955"
	WETH = "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
)

var feeTiers = []uint32{100, 500, 2500, 10000}

// --------- ENV helpers ---------

func envStr(key, def string) string { if v := os.Getenv(key); v != "" { return v }; return def }
func envInt(key string, def int) int { v := strings.TrimSpace(os.Getenv(key)); if v=="" {return def}; i,err:=strconv.Atoi(v); if err!=nil {return def}; return i }
func envDurationMS(key string, defMS int) time.Duration { return time.Duration(envInt(key, defMS)) * time.Millisecond }

// --------- Token info (extend as needed) ---------

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

// --------- Pool version & ref ---------

type poolVer uint8
const (
	verV2 poolVer = 2
	verV3 poolVer = 3
)

type poolRef struct {
	addr    common.Address
	ver     poolVer
	// v2 orientation (for price) — best-effort fetched via token0/token1
	v2t0, v2t1 common.Address
	// event id cached (v2: Swap, v3: Swap)
	eventID common.Hash
}

// --------- Collector ---------

type Collector struct {
	Symbol string
	Pub    publisher.Nats

	ws   *ethclient.Client
	http *ethclient.Client

	v3PoolABI    abi.ABI
	v3FactoryABI abi.ABI
	v2PairABI    abi.ABI

	mu        sync.RWMutex
	lastBlock uint64
	lastIdx   uint

	token0, token1         common.Address
	token0Decimals         int
	token1Decimals         int
	invertForQuote         bool

	confirmations uint64
	logPollEvery  time.Duration

	pools []poolRef
}

func New(symbol string, _ []common.Address, pub publisher.Nats) *Collector { // keep signature for backward compat
	return &Collector{Symbol: strings.ToUpper(symbol), Pub: pub}
}
func (c *Collector) Name() string  { return "PancakeSwap(" + c.Symbol + ")" }
func (c *Collector) Venue() string { return "dex" }

func (c *Collector) Run(ctx context.Context) error {
	// cfg
	wsURL := envStr("BSC_WS_URL", DEFAULT_BSC_WS_URL)
	httpURL := envStr("BSC_HTTP_URL", DEFAULT_BSC_HTTP)
	c.confirmations = uint64(envInt("DEX_CONFIRMATIONS", 3))
	c.logPollEvery = envDurationMS("DEX_LOG_POLL_MS", 2000)

	// connect WS/HTTP
	var err error
	c.ws, err = ethclient.DialContext(ctx, wsURL); if err != nil { return fmt.Errorf("WS dial: %w", err) }
	defer c.ws.Close()
	if httpURL != "" { if hc, err := ethclient.DialContext(ctx, httpURL); err == nil { c.http = hc; defer c.http.Close() } }

	// ABIs
	if c.v3PoolABI, err = abi.JSON(strings.NewReader(pool3ABIJSON)); err != nil { return fmt.Errorf("v3 pool ABI: %w", err) }
	if c.v3FactoryABI, err = abi.JSON(strings.NewReader(pancakeV3FactoryABI)); err != nil { return fmt.Errorf("v3 factory ABI: %w", err) }
	if c.v2PairABI, err = abi.JSON(strings.NewReader(pool2ABIJSON)); err != nil { return fmt.Errorf("v2 pair ABI: %w", err) }

	// tokens & orientation
	if err := c.setupTokenInfo(); err != nil { return err }

	// pools (mixed)
	c.pools = c.loadPools(ctx)
	if len(c.pools) == 0 { return fmt.Errorf("no pools for %s", c.Symbol) }
	log.Printf("[%s] monitoring %d pool(s)", c.Name(), len(c.pools))

	// subscribe each pool via unified path
	for _, ref := range c.pools { r := ref; go c.subscribePool(ctx, r) }
	<-ctx.Done(); return ctx.Err()
}

func (c *Collector) setupTokenInfo() error {
	t0, t1, d0, d1, ok := getTokenInfoForSymbol(c.Symbol); if !ok { return fmt.Errorf("unsupported symbol: %s", c.Symbol) }
	if bytes.Compare(t0.Bytes(), t1.Bytes()) <= 0 { c.token0, c.token1, c.token0Decimals, c.token1Decimals, c.invertForQuote = t0, t1, d0, d1, false } else { c.token0, c.token1, c.token0Decimals, c.token1Decimals, c.invertForQuote = t1, t0, d1, d0, true }
	return nil
}

func (c *Collector) loadPools(ctx context.Context) []poolRef {
	v3, v2 := envPoolsForSymbolV3V2(c.Symbol)
	var out []poolRef
	// v2: cache event id and token0/1
	for _, a := range v2 {
		ref := poolRef{addr: a, ver: verV2, eventID: c.v2PairABI.Events["Swap"].ID}
		// best-effort token0/token1
		if data, err := c.v2PairABI.Pack("token0"); err == nil {
			if res, err := c.ws.CallContract(ctx, ethereum.CallMsg{To: &a, Data: data}, nil); err == nil {
				if vals, err := c.v2PairABI.Unpack("token0", res); err == nil && len(vals)==1 { ref.v2t0, _ = vals[0].(common.Address) }
			}
		}
		if data, err := c.v2PairABI.Pack("token1"); err == nil {
			if res, err := c.ws.CallContract(ctx, ethereum.CallMsg{To: &a, Data: data}, nil); err == nil {
				if vals, err := c.v2PairABI.Unpack("token1", res); err == nil && len(vals)==1 { ref.v2t1, _ = vals[0].(common.Address) }
			}
		}
		out = append(out, ref)
	}
	// v3: from ENV (raw -> v3) or factory discovery
	for _, a := range v3 { out = append(out, poolRef{addr: a, ver: verV3, eventID: c.v3PoolABI.Events["Swap"].ID}) }
	if len(v3) == 0 && len(v2) == 0 {
		for _, a := range c.discoverPoolsViaFactory(ctx) { out = append(out, poolRef{addr: a, ver: verV3, eventID: c.v3PoolABI.Events["Swap"].ID}) }
	}
	return out
}

// ONE subscribe for both versions
func (c *Collector) subscribePool(ctx context.Context, pr poolRef) {
	ch := make(chan types.Log, 256)
	q := ethereum.FilterQuery{Addresses: []common.Address{pr.addr}, Topics: [][]common.Hash{{pr.eventID}}}
	for {
		sub, err := c.ws.SubscribeFilterLogs(ctx, q, ch)
		if err != nil { log.Printf("[%s] sub error %s: %v", c.Name(), pr.addr.Hex(), err); time.Sleep(2*time.Second); continue }
		for {
			select {
			case <-sub.Err():
				sub.Unsubscribe(); time.Sleep(1*time.Second); goto RETRY
			case lg := <-ch:
				c.handleLog(lg, pr)
			case <-ctx.Done():
				sub.Unsubscribe(); return
			}
		}
	RETRY:
	}
}

// ONE handler for both versions
func (c *Collector) handleLog(lg types.Log, pr poolRef) {
	if lg.Removed { return }
	// de-dup
	c.mu.Lock()
	if (lg.BlockNumber < c.lastBlock) || (lg.BlockNumber == c.lastBlock && lg.Index <= c.lastIdx) { c.mu.Unlock(); return }
	c.lastBlock, c.lastIdx = lg.BlockNumber, lg.Index
	c.mu.Unlock()

	switch pr.ver {
	case verV3:
		fields, err := c.v3PoolABI.Events["Swap"].Inputs.NonIndexed().Unpack(lg.Data)
		if err != nil || len(fields) < 5 { return }
		sp, _ := fields[2].(*big.Int); if sp==nil || sp.Sign()==0 { return }
		a0, _ := fields[0].(*big.Int)
		a1, _ := fields[1].(*big.Int)
		liq,_ := fields[3].(*big.Int)
		tk, _ := fields[4].(*big.Int)
		p01, p10 := c.sqrtPriceX96ToPrice(sp)
		evt := publisher.NewMarketDataBuilder("PancakeSwapV3", c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP).
			WithDexSwapL1(&pb.DexSwapL1{
				Amount0: toStr(a0), Amount1: toStr(a1), SqrtPriceX96: toStr(sp),
				Tick: int32(tk.Int64()), LiquidityU128: toStr(liq),
				Pool: lg.Address.Hex(), TxHash: lg.TxHash.Hex(), LogIndex: uint64(lg.Index), BlockNumber: uint64(lg.BlockNumber),
				Token0: c.token0.Hex(), Token1: c.token1.Hex(), Token0Decimals: uint32(c.token0Decimals), Token1Decimals: uint32(c.token1Decimals),
				InvertForQuote: c.invertForQuote, Price_01: p01, Price_10: p10,
			}).Build()
		if evt.Header == nil { evt.Header = &pb.Header{} }
		evt.Header.TsNs = time.Now().UnixNano(); _ = c.Pub.Publish(evt)

	case verV2:
		// V2 Swap(amount0In, amount1In, amount0Out, amount1Out) + indexed sender,to in topics
		fields, err := c.v2PairABI.Events["Swap"].Inputs.NonIndexed().Unpack(lg.Data)
		if err != nil || len(fields) < 4 { return }
		a0In,  _ := fields[0].(*big.Int)
		a1In,  _ := fields[1].(*big.Int)
		a0Out, _ := fields[2].(*big.Int)
		a1Out, _ := fields[3].(*big.Int)

		// pool-side signed deltas to align with v3 semantics: amount = in - out
		amt0 := new(big.Int).Sub(zeroOr(a0In), zeroOr(a0Out))
		amt1 := new(big.Int).Sub(zeroOr(a1In), zeroOr(a1Out))

		// effective trade price token1/token0
		price01, price10 := v2TradePrice(amt0, amt1, a0In, a1In, a0Out, a1Out, c.token0Decimals, c.token1Decimals)

		evt := publisher.NewMarketDataBuilder("PancakeSwapV2", c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP).
			WithDexSwapL1(&pb.DexSwapL1{
				Amount0: amt0.String(), Amount1: amt1.String(),
				SqrtPriceX96: "0", Tick: 0, LiquidityU128: "0",
				Pool: lg.Address.Hex(), TxHash: lg.TxHash.Hex(), LogIndex: uint64(lg.Index), BlockNumber: uint64(lg.BlockNumber),
				Token0: c.token0.Hex(), Token1: c.token1.Hex(), Token0Decimals: uint32(c.token0Decimals), Token1Decimals: uint32(c.token1Decimals),
				InvertForQuote: c.invertForQuote, Price_01: price01, Price_10: price10,
			}).Build()
		if evt.Header == nil { evt.Header = &pb.Header{} }
		evt.Header.TsNs = time.Now().UnixNano(); _ = c.Pub.Publish(evt)
	}
}

// sqrtPriceX96 -> prices (v3)
func (c *Collector) sqrtPriceX96ToPrice(sqrtPriceX96 *big.Int) (p01, p10 float64) {
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 { return 0, 0 }
	prec := uint(256)
	num := new(big.Float).SetPrec(prec).SetInt(sqrtPriceX96)
	q96 := new(big.Float).SetPrec(prec).SetInt(new(big.Int).Lsh(big.NewInt(1), 96))

	ratio := new(big.Float).Quo(num, q96)
	p01bf := new(big.Float).Mul(ratio, ratio)
	p01bf.Mul(p01bf, big.NewFloat(math.Pow10(c.token0Decimals-c.token1Decimals)))
	p01, _ = p01bf.Float64()
	
	rinv := new(big.Float).Quo(q96, num)
	p10bf := new(big.Float).Mul(rinv, rinv)
	p10bf.Mul(p10bf, big.NewFloat(math.Pow10(c.token1Decimals-c.token0Decimals)))
	p10, _ = p10bf.Float64()
	if p01 <= 0 { p01 = 0 }; if p10 <= 0 { p10 = 0 }
	return
}

// v2 price from trade legs (decimals aware)
func v2TradePrice(amt0, amt1, a0In, a1In, a0Out, a1Out *big.Int, d0, d1 int) (price01, price10 float64) {
	prec := uint(256)
	scale := big.NewFloat(math.Pow10(d0 - d1))
	if a0In != nil && a0In.Sign() > 0 && a1Out != nil && a1Out.Sign() > 0 {
		bf := new(big.Float).Quo(new(big.Float).SetPrec(prec).SetInt(a1Out), new(big.Float).SetPrec(prec).SetInt(a0In))
		bf.Mul(bf, scale); price01, _ = bf.Float64()
	} else if a1In != nil && a1In.Sign() > 0 && a0Out != nil && a0Out.Sign() > 0 {
		bf := new(big.Float).Quo(new(big.Float).SetPrec(prec).SetInt(a1In), new(big.Float).SetPrec(prec).SetInt(a0Out))
		bf.Mul(bf, scale); price01, _ = bf.Float64()
	}
	if price01 > 0 { price10 = 1 / price01 }
	return
}

// helpers
func toStr(x *big.Int) string { if x==nil { return "0" }; return x.String() }
func zeroOr(x *big.Int) *big.Int { if x==nil { return big.NewInt(0) }; return new(big.Int).Set(x) }

// --------- ENV parsing (raw -> v3) ---------

func envPoolsForSymbolV3V2(sym string) (v3 []common.Address, v2 []common.Address) {
	key := "DEX_POOLS_" + strings.ToUpper(strings.TrimSpace(sym))
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" { return nil, nil }
	items := strings.Split(raw, ",")
	for _, it := range items {
		it = strings.TrimSpace(it); if it == "" { continue }
		parts := strings.Split(it, "@")
		if len(parts) == 2 {
			ver := strings.ToLower(strings.TrimSpace(parts[0]))
			addr := strings.TrimSpace(parts[1])
			if !common.IsHexAddress(addr) { continue }
			switch ver { case "v3": v3 = append(v3, common.HexToAddress(addr)); case "v2": v2 = append(v2, common.HexToAddress(addr)) }
			continue
		}
		if common.IsHexAddress(it) { v3 = append(v3, common.HexToAddress(it)) }
	}
	return
}

// --------- v3 discovery (only when no ENV v3) ---------

func (c *Collector) discoverPoolsViaFactory(ctx context.Context) []common.Address {
	factory := common.HexToAddress(PANCAKE_V3_FACTORY)
	var found []common.Address
	tokA, tokB := c.token0, c.token1
	for _, fee := range feeTiers {
		calldata, err := c.v3FactoryABI.Pack("getPool", tokA, tokB, new(big.Int).SetUint64(uint64(fee)))
		if err != nil { continue }
		res, err := c.ws.CallContract(ctx, ethereum.CallMsg{To: &factory, Data: calldata}, nil)
		if err != nil { continue }
		out, err := c.v3FactoryABI.Unpack("getPool", res)
		if err != nil || len(out)==0 { continue }
		addr, _ := out[0].(common.Address)
		if addr != (common.Address{}) { found = append(found, addr) }
	}
	return found
}

// (optional) your Multicall/Quoter ABIs can live elsewhere; omitted here for brevity

// --------- init (register unified) ---------

func init() {
	exchange.Register("pancakeswap", func(symbol string, pub publisher.Nats) exchange.Collector {
		return New(symbol, nil, pub) // unified v2+v3
	})
	// keep alias for compatibility
	exchange.Register("pancakeswapv3", func(symbol string, pub publisher.Nats) exchange.Collector {
		return New(symbol, nil, pub)
	})
}
