package publisher

import (
	"time"

	pb "github.com/sujine/market-fetcher/proto"
)

// MarketDataBuilder helps construct pb.MarketData with a builder pattern.
type MarketDataBuilder struct {
	evt *pb.MarketData
}

// NewMarketDataBuilder creates a new builder with header skeleton.
func NewMarketDataBuilder(exchange, symbol string, venue pb.Venue, inst pb.Instrument) *MarketDataBuilder {
	return &MarketDataBuilder{
		evt: &pb.MarketData{
			Header: &pb.Header{
				Exchange:      exchange,
				Symbol:        symbol,
				Venue:         venue,
				Instrument:    inst,
				TsNs:          time.Now().UnixNano(),
				SchemaVersion: 2,
				// Chain / PoolAddress are optional; set via helpers below if needed.
			},
		},
	}
}

// ---------- Header helpers (optional) ----------

// WithHeaderChain sets the chain name for DEX contexts (e.g., "BSC").
func (b *MarketDataBuilder) WithHeaderChain(chain string) *MarketDataBuilder {
	if b.evt != nil && b.evt.Header != nil {
		b.evt.Header.Chain = chain
	}
	return b
}

// WithHeaderPoolAddress sets the raw DEX pool address that produced the tick/trade.
func (b *MarketDataBuilder) WithHeaderPoolAddress(addr string) *MarketDataBuilder {
	if b.evt != nil && b.evt.Header != nil {
		b.evt.Header.PoolAddress = addr
	}
	return b
}

// WithHeaderSchemaVersion lets you override schema version if needed.
func (b *MarketDataBuilder) WithHeaderSchemaVersion(v uint32) *MarketDataBuilder {
	if b.evt != nil && b.evt.Header != nil {
		b.evt.Header.SchemaVersion = v
	}
	return b
}

// WithHeaderTimestamp overrides the event timestamp (ns).
func (b *MarketDataBuilder) WithHeaderTimestamp(ns int64) *MarketDataBuilder {
	if b.evt != nil && b.evt.Header != nil {
		b.evt.Header.TsNs = ns
	}
	return b
}

// ---------- Existing payload helpers ----------

func (b *MarketDataBuilder) WithTick(bid, ask, mid, bidSz, askSz, twap, liq float64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Tick{
		Tick: &pb.TickL1{
			Bid:       bid,
			Ask:       ask,
			Mid:       mid,
			BidSize:   bidSz,
			AskSize:   askSz,
			Twap:      twap,
			Liquidity: liq,
		},
	}
	return b
}

func (b *MarketDataBuilder) WithFunding(mark, index, est, rate float64, nextFund int64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Funding{
		Funding: &pb.Funding{
			MarkPrice:            mark,
			IndexPrice:           index,
			EstimatedSettlePrice: est,
			FundingRate:          rate,
			NextFundingTime:      nextFund,
		},
	}
	return b
}

func (b *MarketDataBuilder) WithFee(maker, taker float64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Fee{
		Fee: &pb.Fee{
			MakerRate: maker,
			TakerRate: taker,
		},
	}
	return b
}

// (선택) 필요하면 Trade도 지원
func (b *MarketDataBuilder) WithTrade(price, size float64, aggr pb.Aggressor) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Trade{
		Trade: &pb.Trade{
			Price:     price,
			Size:      size,
			Aggressor: aggr,
		},
	}
	return b
}

// ---------- NEW: Stats24h (거래량/고저/건수) ----------

func (b *MarketDataBuilder) WithStats24h(volQuote, volBase, high, low float64, trades uint64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Stats{
		Stats: &pb.Stats24H{
			VolumeQuote: volQuote,
			VolumeBase:  volBase,
			High:        high,
			Low:         low,
			Trades:      trades,
		},
	}
	return b
}

// WithDexSwapL1 sets the MarketData oneof to DexSwapL1 using a convenience struct.
// Header.Chain / Header.PoolAddress should be set with WithHeaderChain / WithHeaderPoolAddress if needed.
func (b *MarketDataBuilder) WithDexSwapL1(f *pb.DexSwapL1) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_DexSwapL1{
		DexSwapL1: f,
	}
	return b
}


// SlippageImpact is a small convenience struct mirroring pb.DexSlippage.Impact.
type SlippageImpact struct {
	SizeQuote float64 // e.g., 100, 1000, 10000 (quote currency size)
	ImpactBps float64 // price impact in basis points
}

func (b *MarketDataBuilder) WithDexSlippage(impacts []SlippageImpact, poolFeeBps uint32) *MarketDataBuilder {
	pbImpacts := make([]*pb.DexSlippage_Impact, 0, len(impacts))
	for _, it := range impacts {
		pbImpacts = append(pbImpacts, &pb.DexSlippage_Impact{
			SizeQuote: it.SizeQuote,
			ImpactBps: it.ImpactBps,
		})
	}
	b.evt.Data = &pb.MarketData_Slippage{
		Slippage: &pb.DexSlippage{
			Impacts:     pbImpacts,
			PoolFeeBps:  poolFeeBps,
		},
	}
	return b
}

// Build finalizes the event.
func (b *MarketDataBuilder) Build() *pb.MarketData {
	return b.evt
}
