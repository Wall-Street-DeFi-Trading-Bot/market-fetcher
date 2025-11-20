package publisher

import pb "github.com/sujine/market-fetcher/proto"

type Nats interface {
	Publish(evt *pb.MarketData) error
	PublishAt(evt *pb.MarketData, subject string) error
}
