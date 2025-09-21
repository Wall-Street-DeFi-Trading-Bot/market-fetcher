// Package: pancakeswap
//
// Unified collector for PancakeSwap v2+v3 with ZERO duplicated flows.
// - One subscribe function (subscribePool)
// - One log handler (PublishPrice)
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

type poolVer uint8
const (
	verV2 poolVer = 2
	verV3 poolVer = 3
)

type poolRef struct {   
	addr	common.Address
	ver     poolVer
	// event id cached (v2: Swap, v3: Swap)
	eventID common.Hash
	fee  	uint32
}

// --------- Collector ---------

type Collector struct {
	Symbol 				string
	Pub    				publisher.Nats

	ws   				*ethclient.Client
	http 				*ethclient.Client

	v3PoolABI    		abi.ABI
	v3FactoryABI 		abi.ABI
	v2PairABI    		abi.ABI

	mu        			sync.RWMutex
	lastBlock 			uint64
	lastIdx   			uint

	token0				common.Address
	token1         		common.Address
	token0Decimals      int
	token1Decimals      int
	invertForQuote      bool

	pools 				map[common.Address]poolRef
	exchangeLabelByAddr map[common.Address]string

	lastSqrtV3 map[common.Address]*big.Int 
	lastPx     map[common.Address]struct{ p01, p10 float64 }
}


func New(symbol string, pub publisher.Nats) *Collector {
	v3PoolABI, err := abi.JSON(strings.NewReader(pool3ABIJSON))
    if err != nil { log.Println("v3 pool ABI:", err); return nil }

    v3FactoryABI, err := abi.JSON(strings.NewReader(pancakeV3FactoryABI))
    if err != nil { log.Println("v3 factory ABI:", err); return nil }

    v2PairABI, err := abi.JSON(strings.NewReader(pool2ABIJSON))
    if err != nil { log.Println("v2 pair ABI:", err); return nil }

	return &Collector{
		Symbol: symbol,
		v3PoolABI: v3PoolABI,
		v3FactoryABI: v3FactoryABI,
		v2PairABI: v2PairABI,
		Pub: pub,
		pools:  make(map[common.Address]poolRef),
		exchangeLabelByAddr: make(map[common.Address]string),
		lastSqrtV3: make(map[common.Address]*big.Int),
		lastPx:     make(map[common.Address]struct{ p01, p10 float64 }),
	}
}
func (c *Collector) Name() string  { return "PancakeSwap(" + c.Symbol + ")" }
func (c *Collector) Venue() string { return "dex" }

func (c *Collector) Run(ctx context.Context) error {
	if err := c.Connect(ctx, envStr("BSC_WS_URL", DEFAULT_BSC_WS_URL), envStr("BSC_HTTP_URL", DEFAULT_BSC_HTTP)); err != nil {
        return err
    }
    defer c.Close()
	if err := c.setupTokenInfo(); err != nil { 
		log.Println(err) 
		return nil
	}

	c.pools = c.loadPools(ctx)

	for _, ref := range c.pools {
		r := ref
		go c.subscribePoolWith(ctx, r, func(lg types.Log, pr poolRef) {
			c.onSwapEvent(ctx, lg, pr)
		})
	}
	
	<-ctx.Done()
	return ctx.Err()
}


type swapEvent struct {
	ver        poolVer
	pool       common.Address
	blockNum   uint64
	logIndex   uint64
	txHash     common.Hash
	
	amount0    *big.Int
	amount1    *big.Int
	liquidity  *big.Int
	tick       int32
	sqrtP      *big.Int

	price01    float64 
	price10    float64

	pre01, pre10  float64
	exec01, exec10 float64
	dir            uint8 

    a0In, a1In 	*big.Int
    a0Out,a1Out	*big.Int   
}

func (c *Collector) Connect(ctx context.Context, wsURL, httpURL string) error {
    ws, err := ethclient.DialContext(ctx, wsURL)
    if err != nil { return err }
    c.ws = ws

    if httpURL != "" {
        if hc, err := ethclient.DialContext(ctx, httpURL); err == nil {
            c.http = hc
        }
    }
    return nil
}

func (c *Collector) Close() {
    if c.ws != nil {
        c.ws.Close(); c.ws = nil
    }
    if c.http != nil {
        c.http.Close(); c.http = nil
    }
}

func (c *Collector) publishPrice(ps *swapEvent) {
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" { ex = "PancakeSwap" }

	var sqrtP, liq string
	var tick int32
	if ps.ver == verV3 {
		sqrtP = toStr(ps.sqrtP)
		liq   = toStr(ps.liquidity)
		tick  = ps.tick
	} else {
		sqrtP = "0"; liq = "0"; tick = 0
	}

	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithDexSwapL1(&pb.DexSwapL1{
			Amount0: toStr(ps.amount0), Amount1: toStr(ps.amount1),
			SqrtPriceX96: sqrtP, Tick: tick, LiquidityU128: liq,
			Pool: ps.pool.Hex(), TxHash: ps.txHash.Hex(), LogIndex: ps.logIndex, BlockNumber: ps.blockNum,
			Token0: ADDR_TO_SYMBOL[c.token0.Hex()], Token1: ADDR_TO_SYMBOL[c.token1.Hex()], Token0Decimals: uint32(c.token0Decimals), Token1Decimals: uint32(c.token1Decimals),
			InvertForQuote: c.invertForQuote, Price01: ps.price01, Price10: ps.price10,
		}).Build()

	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishVolume(ps *swapEvent) {
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" { ex = "PancakeSwap" }

	vol0 := absFloat(intToFloat(ps.amount0, c.token0Decimals))
	vol1 := absFloat(intToFloat(ps.amount1, c.token1Decimals))
	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithVolume(vol0, vol1, 0, 0, 0). // 집계는 downstream
		Build()
	
	// fmt.Println("??",  ADDR_TO_SYMBOL[c.token0.Hex()],vol0, ADDR_TO_SYMBOL[c.token1.Hex()], vol1)
	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishFee(ps *swapEvent) {
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" { ex = "PancakeSwap" }

	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithFee(0.0, float64(c.pools[ps.pool].fee)/1e6).
		Build()

	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishSlippage(ps *swapEvent) {
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" { ex = "PancakeSwap" }
	
	var impactBps01, impactBps10 float64
	
	switch ps.dir {
	case Dir01:
		if ps.pre01 > 0 && ps.exec01 > 0 {
			impactBps01 = (ps.exec01/ps.pre01 - 1.0) * 10000.0
		}
	case Dir10:
		if ps.pre10 > 0 && ps.exec10 > 0 {
			impactBps10 = (ps.exec10/ps.pre10 - 1.0) * 10000.0
		}
	default:
	}
	
	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithSlippage(impactBps01, impactBps10, ps.txHash.Hex(), ps.blockNum).
		Build()

	_ = c.Pub.Publish(evt)
}


// ONE subscribe for both versions
func (c *Collector) subscribePoolWith(ctx context.Context, pr poolRef, handler func(types.Log, poolRef)) {
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
				if lg.Removed { continue }
				handler(lg, pr)
			case <-ctx.Done():
				sub.Unsubscribe(); return
			}
		}
	RETRY:
	}
}


func (c *Collector) parseSwap(ctx context.Context, lg types.Log, pr poolRef) (*swapEvent, bool) {
	ps := &swapEvent{
		ver:      pr.ver,
		pool:     lg.Address,
		blockNum: uint64(lg.BlockNumber),
		logIndex: uint64(lg.Index),
		txHash:   lg.TxHash,
	}

	switch pr.ver {
	case verV3:
		fields, err := c.v3PoolABI.Events["Swap"].Inputs.NonIndexed().Unpack(lg.Data)
		if err != nil || len(fields) < 5 { return nil, false }
		a0, _ := fields[0].(*big.Int)
		a1, _ := fields[1].(*big.Int)
		sp, _ := fields[2].(*big.Int)
		if sp == nil || sp.Sign() == 0 { 
			return nil, false 
		}
		liq,_ := fields[3].(*big.Int)
		tk, _ := fields[4].(*big.Int)

		ps.amount0 = a0
		ps.amount1 = a1
		ps.sqrtP   = sp
		ps.liquidity = liq
		if tk != nil { ps.tick = int32(tk.Int64()) }

		p01, p10 := c.sqrtPriceX96ToPrice(sp)
		ps.price01, ps.price10 = p01, p10

	case verV2:
		fields, err := c.v2PairABI.Events["Swap"].Inputs.NonIndexed().Unpack(lg.Data)
		if err != nil || len(fields) < 4 { return nil, false }
		a0In,_ := fields[0].(*big.Int)
		a1In,_ := fields[1].(*big.Int)
		a0Out,_:= fields[2].(*big.Int)
		a1Out,_:= fields[3].(*big.Int)

		ps.a0In, ps.a1In, ps.a0Out, ps.a1Out = a0In, a1In, a0Out, a1Out

		amt0 := new(big.Int).Sub(zeroOr(a0In), zeroOr(a0Out))
		amt1 := new(big.Int).Sub(zeroOr(a1In), zeroOr(a1Out))

		ps.amount0 = amt0
		ps.amount1 = amt1

		p01, p10 := v2TradePrice(amt0, amt1, a0In, a1In, a0Out, a1Out, c.token0Decimals, c.token1Decimals)
		ps.price01, ps.price10 = p01, p10
	}

	return ps, true
}


func (c *Collector) onSwapEvent(ctx context.Context, lg types.Log, pr poolRef) {
	c.mu.Lock()
	if (lg.BlockNumber < c.lastBlock) || (lg.BlockNumber == c.lastBlock && lg.Index <= c.lastIdx) {
		c.mu.Unlock(); return
	}
	c.lastBlock, c.lastIdx = lg.BlockNumber, lg.Index
	c.mu.Unlock()

	ps, ok := c.parseSwap(ctx, lg, pr)
	if !ok { return }

	switch pr.ver {
    case verV3:
		ps.dir = detectDirV3(ps)

        if prev := c.lastSqrtV3[ps.pool]; prev != nil && prev.Sign() > 0 {
            b01, b10 := c.sqrtPriceX96ToPrice(prev)
            ps.pre01, ps.pre10 = b01, b10
        }

		e01, e10 := c.execPreFeeFromLegs(ps)
		ps.exec01, ps.exec10 = e01, e10

		if ps.sqrtP != nil && ps.sqrtP.Sign() > 0 {
            c.lastSqrtV3[ps.pool] = new(big.Int).Set(ps.sqrtP)
        }
	case verV2:
		ps.dir = detectDirV2(ps)

		if lp, ok := c.lastPx[ps.pool]; ok && lp.p01 > 0 && lp.p10 > 0 {
			ps.pre01, ps.pre10 = lp.p01, lp.p10
		}
			
		e01, e10 := c.execPreFeeFromLegs(ps)
		ps.exec01, ps.exec10 = e01, e10

		if e01 > 0 && e10 > 0 {
			c.lastPx[ps.pool] = struct{ p01, p10 float64 }{e01, e10}
		}
	}

	c.publishPrice(ps)
	c.publishVolume(ps)
	c.publishFee(ps)
	c.publishSlippage(ps)
}

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
		bf := new(big.Float).Quo(
				new(big.Float).SetPrec(prec).SetInt(a1Out), 
				new(big.Float).SetPrec(prec).SetInt(a0In),
			)
		bf.Mul(bf, scale); price01, _ = bf.Float64()
	} else if a1In != nil && a1In.Sign() > 0 && a0Out != nil && a0Out.Sign() > 0 {
		bf := new(big.Float).Quo(
				new(big.Float).SetPrec(prec).SetInt(a1In), 
				new(big.Float).SetPrec(prec).SetInt(a0Out),
			)
		bf.Mul(bf, scale); price01, _ = bf.Float64()
	}
	if price01 > 0 { price10 = 1 / price01 }
	return
}

func (c *Collector) setupTokenInfo() error {
	t0, t1, d0, d1, ok := getTokenInfoForSymbol(c.Symbol)
	if !ok {
		return fmt.Errorf("unsupported symbol: %s", c.Symbol)
	}
	if bytes.Compare(t0.Bytes(), t1.Bytes()) <= 0 { 
		c.token0, c.token1, c.token0Decimals, c.token1Decimals, c.invertForQuote = t0, t1, d0, d1, false 
	} else { 
		c.token0, c.token1, c.token0Decimals, c.token1Decimals, c.invertForQuote = t1, t0, d1, d0, true 
	}
	return nil
}

// legs에서 프리-피 실행가(p01,p10) 복원
func (c *Collector) execPreFeeFromLegs(ps *swapEvent) (p01, p10 float64) {
    f := float64(c.pools[ps.pool].fee) / float64(1e6)

    // 0->1
    if ps.amount0 != nil && ps.amount0.Sign() > 0 && ps.amount1 != nil && ps.amount1.Sign() < 0 {
        baseIn := absFloat(intToFloat(ps.amount0, c.token0Decimals))
        quoteOut := absFloat(intToFloat(new(big.Int).Neg(ps.amount1), c.token1Decimals))
        if baseIn > 0 {
            p01 = quoteOut / (baseIn * (1 - f))
            if p01 > 0 { p10 = 1.0 / p01 }
        }
        return
    }
    // 1->0
    if ps.amount1 != nil && ps.amount1.Sign() > 0 && ps.amount0 != nil && ps.amount0.Sign() < 0 {
        baseIn := absFloat(intToFloat(ps.amount1, c.token1Decimals))
        quoteOut := absFloat(intToFloat(new(big.Int).Neg(ps.amount0), c.token0Decimals))
        if baseIn > 0 {
            p10 = quoteOut / (baseIn * (1 - f))
            if p10 > 0 { p01 = 1.0 / p10 }
        }
        return
    }
    return 0,0
}



func (c *Collector) loadPools(ctx context.Context) map[common.Address]poolRef {
	v3, v2 := envPoolsForSymbolV3V2(c.Symbol)

	out := make(map[common.Address]poolRef, len(v3)+len(v2))

	for _, a := range v3 {
		var feeU32 uint32
		if data, err := c.v3PoolABI.Pack("fee"); err == nil {
			if res, err := c.ws.CallContract(ctx, ethereum.CallMsg{To: &a, Data: data}, nil); err == nil {
                if outVals, err := c.v3PoolABI.Unpack("fee", res); err == nil && len(outVals) > 0 {
                    switch v := outVals[0].(type) {
                    case *big.Int:
                        feeU32 = uint32(v.Uint64())
                    case uint32:
                        feeU32 = v
                    case uint64:
                        feeU32 = uint32(v)
                    default:
                        feeU32 = 0
                    }
                }
            }
		}

		out[a] = poolRef{
            addr:    a,
            ver:     verV3,
            eventID: c.v3PoolABI.Events["Swap"].ID,
            fee:     feeU32,
        }

		if c.exchangeLabelByAddr != nil {
			c.exchangeLabelByAddr[a] = "PancakeSwapV3"
		}	
	}

	for _, a := range v2 {
		out[a] = poolRef{
            addr:    a,
            ver:     verV2,
            eventID: c.v2PairABI.Events["Swap"].ID,
            fee:      uint32(envInt("DEX_V2_FEE", 2500)),
        }
		if c.exchangeLabelByAddr != nil {
			c.exchangeLabelByAddr[a] = "PancakeSwapV2"
		}
	}
	return out
}


func init() {
	exchange.Register("pancakeswap", func(symbol string, pub publisher.Nats) exchange.Collector {
		return New(symbol, pub)
	})
}