// package nats: normalized MarketData v2 publisher for NATS
//   - Builds subject from pb.MarketData header + oneof kind automatically.
//   - Subject format:
//     md.<kind>.<venue>.<exchange>[.<chain>].<symbol>.<instrument>
//
// Example:
//
//	md.tick.cex.Binance.ETHUSDT.spot
//	md.funding.cex.Binance.BTCUSDT.perpetual
//	md.tick.dex.PancakeSwapV3.BSC.WBNB-USDT.spot
package nats

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	pb "github.com/sujine/market-fetcher/proto"
)

type Publisher struct {
	nc         *nats.Conn
	flushEvery bool
	flushTO    time.Duration
}

// New creates a NATS publisher with sane reconnect defaults.
// Env:
//   MF_FLUSH_EVERY=1            // enable FlushTimeout after every publish
//   MF_FLUSH_TIMEOUT_MS=50      // flush timeout (ms)
func New(url string) (*Publisher, error) {
	nc, err := nats.Connect(
		url,
		nats.Name("market-fetcher-publisher"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
		nats.PingInterval(10*time.Second),
	)
	if err != nil {
		return nil, err
	}
	flushEvery := os.Getenv("MF_FLUSH_EVERY") == "1"
	flushTOms, _ := strconv.Atoi(os.Getenv("MF_FLUSH_TIMEOUT_MS"))
	if flushTOms <= 0 {
		flushTOms = 50
	}
	return &Publisher{
		nc:         nc,
		flushEvery: flushEvery,
		flushTO:    time.Duration(flushTOms) * time.Millisecond,
	}, nil
}

func (p *Publisher) Close() {
	if p.nc != nil {
		_ = p.nc.Drain()
		p.nc.Close()
	}
}

// Publish marshals evt and publishes it to a subject computed from the header.
// Subject pattern: md.<kind>.<venue>.<exchange>[.<chain>].<symbol>.<instrument>
func (p *Publisher) Publish(evt *pb.MarketData) error {
	if evt == nil {
		return errors.New("nil event")
	}
	if evt.Header == nil {
		return errors.New("nil header")
	}
	if evt.Header.TsNs == 0 {
		evt.Header.TsNs = time.Now().UnixNano()
	}
	subject, err := subjectFor(evt)
	if err != nil {
		return err
	}
	return p.publish(subject, evt)
}

// PublishAt marshals evt and publishes to an explicit subject.
func (p *Publisher) PublishAt(evt *pb.MarketData, subject string) error {
	if evt == nil {
		return errors.New("nil event")
	}
	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	if evt.Header.TsNs == 0 {
		evt.Header.TsNs = time.Now().UnixNano()
	}
	return p.publish(subject, evt)
}

// ---- internals ----

func (p *Publisher) publish(subject string, evt proto.Message) error {
	b, err := proto.Marshal(evt)
	if err != nil {
		return err
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    b,
		Header:  nats.Header{},
	}
	// Local enqueue timestamp (for downstream latency metrics)
	msg.Header.Set("mf-pub-ns", strconv.FormatInt(time.Now().UnixNano(), 10))

	if err := p.nc.PublishMsg(msg); err != nil {
		return err
	}
	if p.flushEvery {
		_ = p.nc.FlushTimeout(p.flushTO)
	}
	return nil
}

// subjectFor builds the subject based on data kind and header fields.
// md.<kind>.<venue>.<exchange>[.<chain>].<symbol>.<instrument>
func subjectFor(evt *pb.MarketData) (string, error) {
    h := evt.Header
    if h.Exchange == "" || h.Symbol == "" {
        return "", fmt.Errorf("missing exchange/symbol")
    }

    kind := ""
    switch evt.Data.(type) {
    case *pb.MarketData_Tick:
        kind = "tick"
    case *pb.MarketData_DexSwapL1:
        kind = "tick"
    case *pb.MarketData_Funding:
        kind = "funding"
    case *pb.MarketData_Fee:
        kind = "fee"
    case *pb.MarketData_Trade:
        kind = "trade"
    case *pb.MarketData_Volume:
        kind = "volume"
    case *pb.MarketData_Slippage:
        kind = "slippage"
    default:
        return "", fmt.Errorf("unknown MarketData kind")
    }

    venue := lower(strings.TrimPrefix(h.Venue.String(), "VENUE_"))        // cex|dex|unspecified
    inst  := lower(strings.TrimPrefix(h.Instrument.String(), "INSTRUMENT_")) // spot|perpetual|swap|...
    if venue == "" { venue = "unspecified" }
    if inst  == "" { inst  = "unspecified" }

    parts := []string{"md", kind, venue, h.Exchange}
    if ch := strings.TrimSpace(h.Chain); ch != "" {
        parts = append(parts, ch) // <- 헤더에 Chain 넣으면 주제에 포함됨
    }
    parts = append(parts, h.Symbol, inst)
    return strings.Join(parts, "."), nil
}


func lower(s string) string { return strings.ToLower(s) }
