package publisher

import pb "github.com/sujine/market-fetcher/proto"

// Publisher abstracts the transport (e.g., NATS).
// publisher.NatsPublisher implements this interface.
type Nats interface {
	Publish(evt *pb.MarketData) error
	PublishAt(evt *pb.MarketData, subject string) error
}
