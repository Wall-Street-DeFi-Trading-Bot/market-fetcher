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
			},
		},
	}
}

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

// Build finalizes the event.
func (b *MarketDataBuilder) Build() *pb.MarketData {
	return b.evt
}
