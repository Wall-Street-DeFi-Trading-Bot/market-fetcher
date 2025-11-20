package binance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/sujine/market-fetcher/pkg/exchange"
	"github.com/sujine/market-fetcher/pkg/internal/wsutil"
	"github.com/sujine/market-fetcher/pkg/publisher"
	pb "github.com/sujine/market-fetcher/proto"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	defSpotMaker = 0.0010 // 0.10%
	defSpotTaker = 0.0010
	defPerpMaker = 0.0002 // 0.02%
	defPerpTaker = 0.0004 // 0.04%
)

// cexEvent carries parsed fields before publishing.
type cexEvent struct {
	ins         pb.Instrument
	sourceTsNs  int64
	publishTsNs int64

	// price (bookTicker)
	bid, ask, mid float64
	bidSz, askSz  float64

	// funding (markPrice)
	mark, index float64
	fundingRate float64
	nextFundMs  int64

	// volume snapshot (aggTrade 1s)
	volBase, volQuote float64
	high, low         float64
	trades            uint64
}

type Collector struct {
	Symbol string
	Pub    publisher.Nats

	exchange string

	// WS endpoints
	spotPriceURL string
	spotAggURL   string
	perpPriceURL string
	perpAggURL   string
	perpMarkURL  string

	// volume aggregators
	spotVol *volAgg
	perpVol *volAgg

	// fee (from env or defaults), re-published periodically
	spotMaker float64
	spotTaker float64
	perpMaker float64
	perpTaker float64

	apiKey    string
	apiSecret string
}

func normalizeSymbol(s string) string {
	r := strings.NewReplacer("/", "", "-", "", "_", "")
	return strings.ToUpper(r.Replace(s))
}

func New(symbol string, pub publisher.Nats) *Collector {
	sym := normalizeSymbol(symbol)
	lower := strings.ToLower(sym)

	return &Collector{
		Symbol:       sym,
		Pub:          pub,
		exchange:     "Binance",
		spotPriceURL: "wss://stream.binance.com:9443/ws/" + lower + "@bookTicker",
		spotAggURL:   "wss://stream.binance.com:9443/ws/" + lower + "@aggTrade",
		perpPriceURL: "wss://fstream.binance.com/ws/" + lower + "@bookTicker",
		perpAggURL:   "wss://fstream.binance.com/ws/" + lower + "@aggTrade",
		perpMarkURL:  "wss://fstream.binance.com/ws/" + lower + "@markPrice@1s",
		spotVol:      newVolAgg(),
		perpVol:      newVolAgg(),
		spotMaker:    envF("BINANCE_SPOT_MAKER", defSpotMaker),
		spotTaker:    envF("BINANCE_SPOT_TAKER", defSpotTaker),
		perpMaker:    envF("BINANCE_PERP_MAKER", defPerpMaker),
		perpTaker:    envF("BINANCE_PERP_TAKER", defPerpTaker),
	}
}

func (c *Collector) Name() string  { return "Binance(" + c.Symbol + ")" }
func (c *Collector) Venue() string { return "cex" }

func (c *Collector) Run(ctx context.Context) error {
	// Prices
	go c.loopBook(ctx, c.spotPriceURL, pb.Instrument_INSTRUMENT_SPOT)
	go c.loopBook(ctx, c.perpPriceURL, pb.Instrument_INSTRUMENT_PERPETUAL)
	// Volumes (1s) from aggTrade
	go c.loopAggTrades(ctx, c.spotAggURL, pb.Instrument_INSTRUMENT_SPOT, c.spotVol)
	go c.loopAggTrades(ctx, c.perpAggURL, pb.Instrument_INSTRUMENT_PERPETUAL, c.perpVol)

	// Funding & next funding time
	go c.loopMark(ctx, c.perpMarkURL, pb.Instrument_INSTRUMENT_PERPETUAL)

	fees := NewFeeStore(c.Symbol, c.Pub)

	go fees.Start(ctx, pollEveryFor(c.Symbol))
	<-ctx.Done()
	return ctx.Err()
}

type bookTicker struct {
	Symbol string `json:"s"`
	Bid    string `json:"b"`
	BidQty string `json:"B"`
	Ask    string `json:"a"`
	AskQty string `json:"A"`
}

type aggTrade struct {
	// Binance aggTrade stream
	EventType    string `json:"e"` // "aggTrade"
	EventTime    int64  `json:"E"` // ms
	Symbol       string `json:"s"`
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Qty          string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	TradeTime    int64  `json:"T"` // ms
	IsBuyerMaker bool   `json:"m"`
	Ignore       bool   `json:"M"`
}

type markPrice struct {
	EventType       string `json:"e"`
	EventTime       int64  `json:"E"`
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	FundingRate     string `json:"r"`
	NextFundingTime int64  `json:"T"`
}

// loopBook consumes bookTicker and publishes best bid/ask.
func (c *Collector) loopBook(ctx context.Context, url string, ins pb.Instrument) {
	wsutil.Run(ctx, url, func(b []byte) {
		// quick guard for non-JSON frames (very rare, but safe)
		if len(b) == 0 || b[0] != '{' {
			return
		}
		v := new(bookTicker)
		if json.Unmarshal(b, v) != nil {
			return
		}
		bid, ask := f64(v.Bid), f64(v.Ask)
		nowNs := time.Now().UnixNano()
		c.publishPrice(&cexEvent{
			ins:         ins,
			sourceTsNs:  nowNs,
			publishTsNs: nowNs,
			bid:         bid, ask: ask, mid: 0.5 * (bid + ask),
			bidSz: f64(v.BidQty), askSz: f64(v.AskQty),
		})
	}, nil)
}

// loopAggTrades aggregates per-second volume (base/quote), OHLC(high/low), #trades.
func (c *Collector) loopAggTrades(ctx context.Context, url string, ins pb.Instrument, a *volAgg) {
	wsutil.Run(ctx, url, func(b []byte) {
		if len(b) == 0 || b[0] != '{' {
			return
		}
		v := new(aggTrade)
		if json.Unmarshal(b, v) != nil {
			return
		}
		sec := v.TradeTime / 1000
		price := f64(v.Price)
		qty := f64(v.Qty)

		if snap, ok := a.add(price, qty, sec); ok {
			srcNs := time.Unix(snap.sec, 0).UnixNano()
			pubNs := time.Now().UnixNano()

			c.publishVolume(&cexEvent{
				ins:         ins,
				sourceTsNs:  srcNs,
				publishTsNs: pubNs,
				volBase:     snap.volBase,
				volQuote:    snap.volQuote,
				high:        snap.high,
				low:         snap.low,
				trades:      snap.trades,
			})
		}
	}, nil)
}

// loopMark consumes markPrice (funding rate & next funding).
func (c *Collector) loopMark(ctx context.Context, url string, ins pb.Instrument) {
	wsutil.Run(ctx, url, func(b []byte) {
		if len(b) == 0 || b[0] != '{' {
			return
		}
		v := new(markPrice)
		if json.Unmarshal(b, v) != nil {
			fmt.Println(json.Unmarshal(b, v))
			return
		}
		srcNs := time.UnixMilli(v.EventTime).UnixNano()
		pubNs := time.Now().UnixNano()

		c.publishFunding(&cexEvent{
			ins:         ins,
			sourceTsNs:  srcNs,
			publishTsNs: pubNs,
			mark:        f64(v.MarkPrice),
			index:       f64(v.IndexPrice),
			fundingRate: f64(v.FundingRate),
			nextFundMs:  v.NextFundingTime,
		})
	}, nil)
}

func (c *Collector) republishFees(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now().UnixNano()
			c.publishFee(&cexEvent{
				ins:         pb.Instrument_INSTRUMENT_SPOT,
				sourceTsNs:  now,
				publishTsNs: now,
			})
			c.publishFee(&cexEvent{
				ins:         pb.Instrument_INSTRUMENT_PERPETUAL,
				sourceTsNs:  now,
				publishTsNs: now,
			})
		}
	}
}

func (c *Collector) publishPrice(ps *cexEvent) {
	evt := publisher.NewMarketDataBuilder(
		c.exchange, c.Symbol, pb.Venue_VENUE_CEX, ps.ins,
	).WithTick(ps.bid, ps.ask, ps.mid, ps.bidSz, ps.askSz, 0, 0).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.SourceTsNs = ps.sourceTsNs
	evt.Header.PublishTsNs = ps.publishTsNs
	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishFunding(ps *cexEvent) {
	evt := publisher.NewMarketDataBuilder(
		c.exchange, c.Symbol, pb.Venue_VENUE_CEX, ps.ins,
	).WithFunding(ps.mark, ps.index, ps.fundingRate, ps.nextFundMs).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.SourceTsNs = ps.sourceTsNs
	evt.Header.PublishTsNs = ps.publishTsNs
	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishVolume(ps *cexEvent) {
	ev := &pb.MarketData{
		Header: &pb.Header{
			Exchange:      c.exchange,
			Symbol:        c.Symbol,
			Venue:         pb.Venue_VENUE_CEX,
			Instrument:    ps.ins,
			SourceTsNs:    ps.sourceTsNs,
			PublishTsNs:   ps.publishTsNs,
			SchemaVersion: 1,
		},
		Data: &pb.MarketData_Volume{
			Volume: &pb.Volume{
				Volume0: ps.volBase,  // base volume = Σ q
				Volume1: ps.volQuote, // quote volume = Σ (p*q)
				High:    ps.high,
				Low:     ps.low,
				Trades:  ps.trades,
			},
		},
	}
	_ = c.Pub.Publish(ev)
}

func (c *Collector) publishFee(ps *cexEvent) {
	maker, taker := c.spotMaker, c.spotTaker
	if ps.ins == pb.Instrument_INSTRUMENT_PERPETUAL {
		maker, taker = c.perpMaker, c.perpTaker
	}
	evt := publisher.NewMarketDataBuilder(
		c.exchange, c.Symbol, pb.Venue_VENUE_CEX, ps.ins,
	).WithFee(maker, taker).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.SourceTsNs = ps.sourceTsNs
	evt.Header.PublishTsNs = ps.publishTsNs
	_ = c.Pub.Publish(evt)
}

type volAgg struct {
	mu       sync.Mutex
	sec      int64
	volBase  float64
	volQuote float64
	high     float64
	low      float64
	trades   uint64
}

type volSnap struct {
	sec      int64
	volBase  float64
	volQuote float64
	high     float64
	low      float64
	trades   uint64
}

func newVolAgg() *volAgg { return &volAgg{} }

// add aggregates by second; returns a snapshot when second rolls over.
func (a *volAgg) add(price, qty float64, sec int64) (volSnap, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.sec == 0 {
		a.sec = sec
		a.high, a.low = price, price
	}

	if sec != a.sec {
		snap := volSnap{
			sec:      a.sec,
			volBase:  a.volBase,
			volQuote: a.volQuote,
			high:     a.high,
			low:      a.low,
			trades:   a.trades,
		}
		// start new second
		a.sec = sec
		a.volBase = qty
		a.volQuote = price * qty
		a.high, a.low = price, price
		a.trades = 1
		return snap, true
	}

	a.volBase += qty
	a.volQuote += price * qty
	if price > a.high {
		a.high = price
	}
	if price < a.low {
		a.low = price
	}
	a.trades++
	return volSnap{}, false
}

func f64(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func init() {
	exchange.Register("binance", func(symbol string, pub publisher.Nats) exchange.Collector {
		return New(symbol, pub)
	})
}
