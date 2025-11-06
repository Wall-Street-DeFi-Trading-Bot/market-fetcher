package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/sujine/market-fetcher/replayer"
)

func main() {
	_ = godotenv.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := replayer.New()
	if err != nil {
		log.Fatalf("replayer init: %v", err)
	}

	go func() {
		if err := r.Run(ctx); err != nil {
			log.Printf("replayer stopped: %v", err)
		}
	}()

	// wait signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
}
