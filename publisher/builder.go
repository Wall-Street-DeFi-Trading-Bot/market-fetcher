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
		},
	}
	return b
}

func (b *MarketDataBuilder) WithFunding(mark, index, rate float64, nextFund int64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Funding{
		Funding: &pb.Funding{
			MarkPrice:            mark,
			IndexPrice:           index,
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

func (b *MarketDataBuilder) WithVolume(vol0, vol1, high, low float64, trades uint64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Volume{
		Volume: &pb.Volume{
			Volume0:    vol0,
			Volume1:    vol1,
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


func (b *MarketDataBuilder) WithSlippage(impact01, impact10 float64, txHash string, blockNum uint64) *MarketDataBuilder {
	b.evt.Data = &pb.MarketData_Slippage{
		Slippage: &pb.Slippage{
			ImpactBps01:     impact01,
			ImpactBps10:  	 impact10,
			TxHash: txHash,
			BlockNumber: blockNum,
		},
	}
	return b
}

// Build finalizes the event.
func (b *MarketDataBuilder) Build() *pb.MarketData {
	return b.evt
}
