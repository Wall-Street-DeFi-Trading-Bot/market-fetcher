package binance

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/sujine/market-fetcher/exchange"
	"github.com/sujine/market-fetcher/internal/wsutil"
	pb "github.com/sujine/market-fetcher/proto"
	"github.com/sujine/market-fetcher/publisher"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	bookTickerPool = sync.Pool{
		New: func() interface{} { return new(bookTicker) },
	}
	ticker24hPool = sync.Pool{
		New: func() interface{} { return new(ticker24h) },
	}
	markPricePool = sync.Pool{
		New: func() interface{} { return new(markPrice) },
	}
)

type Collector struct {
	Symbol string
	Pub    publisher.Nats

	spotBookURL   string
	spotTickerURL string
	perpBookURL   string
	perpTickerURL string
	perpMarkURL   string

	exchange string
}

func New(symbol string, pub publisher.Nats) *Collector {
	sym := strings.ToUpper(symbol)
	lower := strings.ToLower(sym)
	return &Collector{
		Symbol:        sym,
		Pub:           pub,
		exchange:      "Binance",
		spotBookURL:   "wss://stream.binance.com:9443/ws/" + lower + "@bookTicker",
		spotTickerURL: "wss://stream.binance.com:9443/ws/" + lower + "@ticker",
		perpBookURL:   "wss://fstream.binance.com/ws/" + lower + "@bookTicker",
		perpTickerURL: "wss://fstream.binance.com/ws/" + lower + "@ticker",
		perpMarkURL:   "wss://fstream.binance.com/ws/" + lower + "@markPrice@1s",
	}
}

func (c *Collector) Name() string  { return "Binance(" + c.Symbol + ")" }
func (c *Collector) Venue() string { return "cex" }

func (c *Collector) Run(ctx context.Context) error {
	go c.loopSpotBook(ctx)
	go c.loopSpotTicker24h(ctx)
	go c.loopPerpBook(ctx)
	go c.loopPerpTicker24h(ctx)
	go c.loopPerpMark(ctx)
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
type ticker24h struct {
	Symbol    string `json:"s"`
	QuoteVol  string `json:"q"`
	Volume    string `json:"v"`
	Open      string `json:"o"`
	High      string `json:"h"`
	Low       string `json:"l"`
	Close     string `json:"c"`
	Pct       string `json:"P"`
	Count     int64  `json:"n"`
	OpenTime  int64  `json:"O"`
	CloseTime int64  `json:"C"`
}
type markPrice struct {
	EventTime  int64  `json:"E"`
	Symbol     string `json:"s"`
	MarkPrice  string `json:"p"`
	IndexPrice string `json:"i"`
	EstPrice   string `json:"P"`
	Funding    string `json:"r"`
	NextFundMs int64  `json### Action Plan`
}

func (c *Collector) publishTick(ins pb.Instrument, bid, ask, mid, bidSz, askSz float64, tsNs int64) {
	evt := publisher.NewMarketDataBuilder(
		c.exchange, c.Symbol, pb.Venue_VENUE_CEX, ins,
	).WithTick(bid, ask, mid, bidSz, askSz, 0, 0).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.TsNs = tsNs
	_ = c.Pub.Publish(evt)
}

func (c *Collector) publishFunding(ins pb.Instrument, m *markPrice, tsNs int64) {
	evt := publisher.NewMarketDataBuilder(
		c.exchange, c.Symbol, pb.Venue_VENUE_CEX, pb.Instrument_INSTRUMENT_PERPETUAL, // Note: Perpetual for funding
	).WithFunding(
		f64(m.MarkPrice), f64(m.IndexPrice), f64(m.EstPrice),
		f64(m.Funding), m.NextFundMs,
	).Build()

	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.TsNs = tsNs
	_ = c.Pub.Publish(evt)
}

func (c *Collector) loopSpotBook(ctx context.Context) {
	wsutil.Run(ctx, c.spotBookURL, func(b []byte) {
		recvNs := time.Now().UnixNano()
		v := bookTickerPool.Get().(*bookTicker)
		defer bookTickerPool.Put(v)   

		if json.Unmarshal(b, v) != nil {
			return
		}
		bid, ask := f64(v.Bid), f64(v.Ask)
		c.publishTick(pb.Instrument_INSTRUMENT_SPOT, bid, ask, 0.5*(bid+ask), f64(v.BidQty), f64(v.AskQty), recvNs)
	}, nil)
}

func (c *Collector) loopSpotTicker24h(ctx context.Context) {
	wsutil.Run(ctx, c.spotTickerURL, func(b []byte) {
		recvNs := time.Now().UnixNano()
		v := ticker24hPool.Get().(*ticker24h)
		defer ticker24hPool.Put(v)

		if json.Unmarshal(b, &v) != nil {
			return
		}
		last := f64(v.Close)
		c.publishTick(pb.Instrument_INSTRUMENT_SPOT, 0, 0, last, 0, 0, recvNs)
	}, nil)
}

func (c *Collector) loopPerpBook(ctx context.Context) {
	wsutil.Run(ctx, c.perpBookURL, func(b []byte) {
		recvNs := time.Now().UnixNano()
		v := bookTickerPool.Get().(*bookTicker)
		defer bookTickerPool.Put(v)

		if json.Unmarshal(b, v) != nil {
			return
		}
		bid, ask := f64(v.Bid), f64(v.Ask)
		c.publishTick(pb.Instrument_INSTRUMENT_PERPETUAL, bid, ask, 0.5*(bid+ask), f64(v.BidQty), f64(v.AskQty), recvNs)
	}, nil)
}

func (c *Collector) loopPerpTicker24h(ctx context.Context) {
	wsutil.Run(ctx, c.perpTickerURL, func(b []byte) {
		recvNs := time.Now().UnixNano()
		v := ticker24hPool.Get().(*ticker24h)
		defer ticker24hPool.Put(v)

		if json.Unmarshal(b, &v) != nil {
			return
		}
		last := f64(v.Close)
		c.publishTick(pb.Instrument_INSTRUMENT_PERPETUAL, 0, 0, last, 0, 0, recvNs)
	}, nil)
}

func (c *Collector) loopPerpMark(ctx context.Context) {
	wsutil.Run(ctx, c.perpMarkURL, func(b []byte) {
		recvNs := time.Now().UnixNano()
		v := markPricePool.Get().(*markPrice)
		defer markPricePool.Put(v)

		if json.Unmarshal(b, v) != nil {
			return
		}
		c.publishFunding(pb.Instrument_INSTRUMENT_PERPETUAL, v, recvNs)
	}, nil)
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
