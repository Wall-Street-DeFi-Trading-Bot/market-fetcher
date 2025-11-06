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

	// remove pool state if block gap is bigger than this
	MEMORY_CLEANUP_THRESHOLD = int64(1000)
)

type poolRef struct {
	addr    common.Address
	ver     poolVer
	eventID common.Hash
	fee     uint32
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

	// global lock
	mu sync.RWMutex

	// per-pool dedupe state
	poolState map[common.Address]struct {
		lastBlock uint64
		lastIdx   uint
	}

	token0              common.Address
	token1              common.Address
	token0Decimals      int
	token1Decimals      int
	invertForQuote      bool
	pools               map[common.Address]poolRef
	exchangeLabelByAddr map[common.Address]string

	// v3 last sqrt, v2 last price
	lastSqrtV3      map[common.Address]*big.Int
	lastPx          map[common.Address]struct{ p01, p10 float64 }
	lastBlockNumber uint64 // for cleanup

	// block number -> unix seconds cache (for on-chain timestamp)
	blockTimeCache map[uint64]uint64
}

func New(symbol string, pub publisher.Nats) *Collector {
	v3PoolABI, err := abi.JSON(strings.NewReader(pool3ABIJSON))
	if err != nil {
		log.Println("v3 pool ABI:", err)
		return nil
	}

	v3FactoryABI, err := abi.JSON(strings.NewReader(pancakeV3FactoryABI))
	if err != nil {
		log.Println("v3 factory ABI:", err)
		return nil
	}

	v2PairABI, err := abi.JSON(strings.NewReader(pool2ABIJSON))
	if err != nil {
		log.Println("v2 pair ABI:", err)
		return nil
	}

	return &Collector{
		Symbol:              symbol,
		v3PoolABI:           v3PoolABI,
		v3FactoryABI:        v3FactoryABI,
		v2PairABI:           v2PairABI,
		Pub:                 pub,
		pools:               make(map[common.Address]poolRef),
		exchangeLabelByAddr: make(map[common.Address]string),
		lastSqrtV3:          make(map[common.Address]*big.Int),
		lastPx:              make(map[common.Address]struct{ p01, p10 float64 }),
		poolState: make(map[common.Address]struct {
			lastBlock uint64
			lastIdx   uint
		}),
		blockTimeCache: make(map[uint64]uint64),
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

	// periodic cleanup
	go c.cleanupPoolStateRoutine(ctx)

	for _, ref := range c.pools {
		r := ref
		go c.subscribePoolWith(ctx, r, func(lg types.Log, pr poolRef) {
			c.onSwapEvent(ctx, lg, pr)
		})
	}

	<-ctx.Done()
	return ctx.Err()
}

func (c *Collector) cleanupPoolStateRoutine(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanupOldPoolState()
		case <-ctx.Done():
			return
		}
	}
}

func (c *Collector) cleanupOldPoolState() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lastBlockNumber == 0 {
		return
	}

	for addr, state := range c.poolState {
		if int64(c.lastBlockNumber)-int64(state.lastBlock) > MEMORY_CLEANUP_THRESHOLD {
			delete(c.poolState, addr)
			delete(c.lastSqrtV3, addr)
			delete(c.lastPx, addr)
		}
	}
}

type swapEvent struct {
	ver            poolVer
	pool           common.Address
	blockNum       uint64
	logIndex       uint64
	txHash         common.Hash
	amount0        *big.Int
	amount1        *big.Int
	liquidity      *big.Int
	tick           int32
	sqrtP          *big.Int
	price01        float64
	price10        float64
	pre01, pre10   float64
	exec01, exec10 float64
	dir            uint8
	a0In, a1In     *big.Int
	a0Out, a1Out   *big.Int
	slippageBps    float64
}

func (c *Collector) Connect(ctx context.Context, wsURL, httpURL string) error {
	ws, err := ethclient.DialContext(ctx, wsURL)
	if err != nil {
		return fmt.Errorf("ws connect failed: %w", err)
	}
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
		c.ws.Close()
		c.ws = nil
	}
	if c.http != nil {
		c.http.Close()
		c.http = nil
	}
}

// getBlockTimeSec returns block timestamp (unix seconds) using cached value if available.
func (c *Collector) getBlockTimeSec(blockNum uint64) uint64 {
	c.mu.RLock()
	if ts, ok := c.blockTimeCache[blockNum]; ok {
		c.mu.RUnlock()
		return ts
	}
	c.mu.RUnlock()

	if c.http == nil {
		return 0
	}

	blk, err := c.http.BlockByNumber(context.Background(), big.NewInt(int64(blockNum)))
	if err != nil || blk == nil {
		return 0
	}
	ts := blk.Time() // seconds

	c.mu.Lock()
	c.blockTimeCache[blockNum] = ts
	c.mu.Unlock()
	return ts
}

func (c *Collector) publishPrice(ps *swapEvent) {
	c.mu.RLock()
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" {
		ex = "PancakeSwap"
	}
	d0 := c.token0Decimals
	d1 := c.token1Decimals
	token0Sym := ADDR_TO_SYMBOL[c.token0.Hex()]
	token1Sym := ADDR_TO_SYMBOL[c.token1.Hex()]
	c.mu.RUnlock()

	var sqrtP, liq string
	var tick int32

	if ps.ver == verV3 {
		sqrtP = toStr(ps.sqrtP)
		liq = toStr(ps.liquidity)
		tick = ps.tick
	} else {
		sqrtP = "0"
		liq = "0"
		tick = 0
	}

	// on-chain time
	onchainSec := c.getBlockTimeSec(ps.blockNum)
	onchainNs := int64(onchainSec) * int64(time.Second)

	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithHeaderTimestamp(onchainNs).
		WithDexSwapL1(&pb.DexSwapL1{
			Amount0:        toStr(ps.amount0),
			Amount1:        toStr(ps.amount1),
			SqrtPriceX96:   sqrtP,
			Tick:           tick,
			LiquidityU128:  liq,
			TxHash:         ps.txHash.Hex(),
			LogIndex:       ps.logIndex,
			BlockNumber:    ps.blockNum,
			Token0:         token0Sym,
			Token1:         token1Sym,
			Token0Decimals: uint32(d0),
			Token1Decimals: uint32(d1),
			InvertForQuote: c.invertForQuote,
			Price01:        ps.price01,
			Price10:        ps.price10,
		}).Build()

	if err := c.Pub.Publish(evt); err != nil {
		log.Printf("[%s] publish price failed: %v", c.Name(), err)
	}
}

func (c *Collector) publishVolume(ps *swapEvent) {
	c.mu.RLock()
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" {
		ex = "PancakeSwap"
	}
	d0 := c.token0Decimals
	d1 := c.token1Decimals
	c.mu.RUnlock()

	vol0 := absFloat(intToFloat(ps.amount0, d0))
	vol1 := absFloat(intToFloat(ps.amount1, d1))

	// on-chain time
	onchainSec := c.getBlockTimeSec(ps.blockNum)
	onchainNs := int64(onchainSec) * int64(time.Second)

	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithHeaderTimestamp(onchainNs).
		WithVolume(vol0, vol1, 0, 0, 0).
		Build()

	if err := c.Pub.Publish(evt); err != nil {
		log.Printf("[%s] publish volume failed: %v", c.Name(), err)
	}
}

func (c *Collector) publishFee(ps *swapEvent) {
	c.mu.RLock()
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" {
		ex = "PancakeSwap"
	}
	pf := c.pools[ps.pool].fee
	c.mu.RUnlock()

	// on-chain time
	onchainSec := c.getBlockTimeSec(ps.blockNum)
	onchainNs := int64(onchainSec) * int64(time.Second)

	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithHeaderTimestamp(onchainNs).
		WithFee(0.0, float64(pf)/1e6).
		Build()

	if err := c.Pub.Publish(evt); err != nil {
		log.Printf("[%s] publish fee failed: %v", c.Name(), err)
	}
}

func (c *Collector) publishSlippage(ps *swapEvent) {
	c.mu.RLock()
	ex := c.exchangeLabelByAddr[ps.pool]
	if ex == "" {
		ex = "PancakeSwap"
	}
	c.mu.RUnlock()

	var impactBps01, impactBps10 float64

	if ps.dir == Dir01 {
		if ps.pre01 > 0 && ps.exec01 > 0 {
			impactBps01 = (ps.exec01/ps.pre01 - 1.0) * 10000.0
		}
	} else if ps.dir == Dir10 {
		if ps.pre10 > 0 && ps.exec10 > 0 {
			impactBps10 = (ps.exec10/ps.pre10 - 1.0) * 10000.0
		}
	}

	// on-chain time
	onchainSec := c.getBlockTimeSec(ps.blockNum)
	onchainNs := int64(onchainSec) * int64(time.Second)

	evt := publisher.NewMarketDataBuilder(
		ex, c.Symbol, pb.Venue_VENUE_DEX, pb.Instrument_INSTRUMENT_SWAP,
	).WithHeaderChain("BSC").
		WithHeaderPoolAddress(ps.pool.Hex()).
		WithHeaderTimestamp(onchainNs).
		WithSlippage(impactBps01, impactBps10, ps.txHash.Hex(), ps.blockNum).
		Build()

	if err := c.Pub.Publish(evt); err != nil {
		log.Printf("[%s] publish slippage failed: %v", c.Name(), err)
	}
}

// --------- subscribe & event handling ---------

func (c *Collector) subscribePoolWith(ctx context.Context, pr poolRef, handler func(types.Log, poolRef)) {
	ch := make(chan types.Log, 256)
	q := ethereum.FilterQuery{
		Addresses: []common.Address{pr.addr},
		Topics:    [][]common.Hash{{pr.eventID}},
	}

	for {
		sub, err := c.ws.SubscribeFilterLogs(ctx, q, ch)
		if err != nil {
			log.Printf("[%s] subscribe error %s: %v", c.Name(), pr.addr.Hex(), err)
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
				handler(lg, pr)
			case <-ctx.Done():
				sub.Unsubscribe()
				return
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
		if err != nil || len(fields) < 5 {
			return nil, false
		}

		a0, _ := fields[0].(*big.Int)
		a1, _ := fields[1].(*big.Int)
		sp, _ := fields[2].(*big.Int)

		if sp == nil || sp.Sign() == 0 {
			return nil, false
		}

		liq, _ := fields[3].(*big.Int)
		tk, _ := fields[4].(*big.Int)

		ps.amount0 = a0
		ps.amount1 = a1
		ps.sqrtP = sp
		ps.liquidity = liq
		if tk != nil {
			ps.tick = int32(tk.Int64())
		}

		p01, p10 := c.sqrtPriceX96ToPrice(sp)
		ps.price01, ps.price10 = p01, p10

	case verV2:
		fields, err := c.v2PairABI.Events["Swap"].Inputs.NonIndexed().Unpack(lg.Data)
		if err != nil || len(fields) < 4 {
			return nil, false
		}

		a0In, _ := fields[0].(*big.Int)
		a1In, _ := fields[1].(*big.Int)
		a0Out, _ := fields[2].(*big.Int)
		a1Out, _ := fields[3].(*big.Int)

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
	// per-pool dedupe
	c.mu.Lock()
	st := c.poolState[lg.Address]
	if uint64(lg.BlockNumber) < st.lastBlock || (uint64(lg.BlockNumber) == st.lastBlock && uint(lg.Index) <= st.lastIdx) {
		c.mu.Unlock()
		return
	}

	c.poolState[lg.Address] = struct {
		lastBlock uint64
		lastIdx   uint
	}{lastBlock: uint64(lg.BlockNumber), lastIdx: uint(lg.Index)}

	c.lastBlockNumber = uint64(lg.BlockNumber)
	c.mu.Unlock()

	ps, ok := c.parseSwap(ctx, lg, pr)
	if !ok {
		return
	}

	switch pr.ver {
	case verV3:
		ps.dir = detectDirV3(ps)

		c.mu.RLock()
		prev := c.lastSqrtV3[ps.pool]
		c.mu.RUnlock()

		if prev != nil && prev.Sign() > 0 {
			b01, b10 := c.sqrtPriceX96ToPrice(prev)
			ps.pre01, ps.pre10 = b01, b10
		}

		e01, e10 := c.execPreFeeFromLegs(ps)
		ps.exec01, ps.exec10 = e01, e10

		if ps.dir == Dir01 && ps.pre01 > 0 && ps.exec01 > 0 {
			ps.slippageBps = (ps.exec01/ps.pre01 - 1.0) * 10000.0
		} else if ps.dir == Dir10 && ps.pre10 > 0 && ps.exec10 > 0 {
			ps.slippageBps = (ps.exec10/ps.pre10 - 1.0) * 10000.0
		}

		if ps.sqrtP != nil && ps.sqrtP.Sign() > 0 {
			c.mu.Lock()
			c.lastSqrtV3[ps.pool] = new(big.Int).Set(ps.sqrtP)
			c.mu.Unlock()
		}

	case verV2:
		ps.dir = detectDirV2(ps)

		c.mu.RLock()
		if lp, ok2 := c.lastPx[ps.pool]; ok2 && lp.p01 > 0 && lp.p10 > 0 {
			ps.pre01, ps.pre10 = lp.p01, lp.p10
		}
		c.mu.RUnlock()

		e01, e10 := c.execPreFeeFromLegs(ps)
		ps.exec01, ps.exec10 = e01, e10

		if ps.dir == Dir01 && ps.pre01 > 0 && ps.exec01 > 0 {
			ps.slippageBps = (ps.exec01/ps.pre01 - 1.0) * 10000.0
		} else if ps.dir == Dir10 && ps.pre10 > 0 && ps.exec10 > 0 {
			ps.slippageBps = (ps.exec10/ps.pre10 - 1.0) * 10000.0
		}

		if e01 > 0 && e10 > 0 {
			c.mu.Lock()
			c.lastPx[ps.pool] = struct{ p01, p10 float64 }{e01, e10}
			c.mu.Unlock()
		}
	}

	c.publishPrice(ps)
	c.publishVolume(ps)
	c.publishFee(ps)
	c.publishSlippage(ps)
}

func (c *Collector) sqrtPriceX96ToPrice(sqrtPriceX96 *big.Int) (p01, p10 float64) {
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
		return 0, 0
	}

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

	if p01 <= 0 {
		p01 = 0
	}
	if p10 <= 0 {
		p10 = 0
	}

	return
}

func v2TradePrice(amt0, amt1, a0In, a1In, a0Out, a1Out *big.Int, d0, d1 int) (price01, price10 float64) {
	prec := uint(256)
	scale := big.NewFloat(math.Pow10(d0 - d1))

	if a0In != nil && a0In.Sign() > 0 && a1Out != nil && a1Out.Sign() > 0 {
		bf := new(big.Float).Quo(
			new(big.Float).SetPrec(prec).SetInt(a1Out),
			new(big.Float).SetPrec(prec).SetInt(a0In),
		)
		bf.Mul(bf, scale)
		price01, _ = bf.Float64()
	} else if a1In != nil && a1In.Sign() > 0 && a0Out != nil && a0Out.Sign() > 0 {
		bf := new(big.Float).Quo(
			new(big.Float).SetPrec(prec).SetInt(a1In),
			new(big.Float).SetPrec(prec).SetInt(a0Out),
		)
		bf.Mul(bf, scale)
		price01, _ = bf.Float64()
	}

	if price01 > 0 {
		price10 = 1 / price01
	}

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

func (c *Collector) execPreFeeFromLegs(ps *swapEvent) (p01, p10 float64) {
	c.mu.RLock()
	feeU := c.pools[ps.pool].fee
	d0 := c.token0Decimals
	d1 := c.token1Decimals
	c.mu.RUnlock()

	f := float64(feeU) / float64(1e6)

	// 0 -> 1
	if ps.amount0 != nil && ps.amount0.Sign() > 0 && ps.amount1 != nil && ps.amount1.Sign() < 0 {
		baseIn := absFloat(intToFloat(ps.amount0, d0))
		quoteOut := absFloat(intToFloat(new(big.Int).Neg(ps.amount1), d1))

		if baseIn > 0 {
			p01 = quoteOut / (baseIn * (1 - f))
			if p01 > 0 {
				p10 = 1.0 / p01
			}
		}
		return
	}
	// 1 -> 0
	if ps.amount1 != nil && ps.amount1.Sign() > 0 && ps.amount0 != nil && ps.amount0.Sign() < 0 {
		baseIn := absFloat(intToFloat(ps.amount1, d1))
		quoteOut := absFloat(intToFloat(new(big.Int).Neg(ps.amount0), d0))

		if baseIn > 0 {
			p10 = quoteOut / (baseIn * (1 - f))
			if p10 > 0 {
				p01 = 1.0 / p10
			}
		}
		return
	}

	return 0, 0
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

		c.mu.Lock()
		c.exchangeLabelByAddr[a] = "PancakeSwapV3"
		c.mu.Unlock()
	}

	for _, a := range v2 {
		out[a] = poolRef{
			addr:    a,
			ver:     verV2,
			eventID: c.v2PairABI.Events["Swap"].ID,
			fee:     uint32(envInt("DEX_V2_FEE", 2500)),
		}

		c.mu.Lock()
		c.exchangeLabelByAddr[a] = "PancakeSwapV2"
		c.mu.Unlock()
	}

	return out
}

func init() {
	exchange.Register("pancakeswap", func(symbol string, pub publisher.Nats) exchange.Collector {
		return New(symbol, pub)
	})
}
