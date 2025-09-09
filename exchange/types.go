package exchange

import (
	"context"
	"fmt"
	"strings"

	"github.com/sujine/market-fetcher/publisher"
)

// Collector is a long-running source that publishes market data via Publisher.
type Collector interface {
	Name() string
	Run(ctx context.Context) error
	Venue() string
}

// Factory constructs a Collector for a given symbol on a given exchange.
type Factory func(symbol string, pub publisher.Nats) Collector

// registry holds exchange -> factory
var registry = map[string]Factory{}

// Register is called by each exchange package in an init() function.
func Register(name string, f Factory) {
	key := strings.ToLower(strings.TrimSpace(name))
	registry[key] = f
}

// NewCollectors creates collectors for the given exchange name and symbols.
func NewCollectors(exchangeName string, symbols []string, pub publisher.Nats) ([]Collector, error) {
	key := strings.ToLower(strings.TrimSpace(exchangeName))
	f, ok := registry[key]
	if !ok {
		return nil, fmt.Errorf("exchange not registered: %s", exchangeName)
	}
	var out []Collector
	for _, s := range symbols {
		if sym := strings.TrimSpace(s); sym != "" {
			out = append(out, f(sym, pub))
		}
	}
	return out, nil
}
