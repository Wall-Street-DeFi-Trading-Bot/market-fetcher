// cmd/tx-replayer/main.go
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
	// load .env from current dir (ignore error if missing)
	_ = godotenv.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1) optionally start local fork (FORK_AUTO_START=1)
	forkURL, cleanup, err := replayer.StartForkFromEnv(ctx)
	if err != nil {
		log.Fatalf("start fork failed: %v", err)
	}
	defer cleanup()

	// 2) build replayer (reads REPLAYER_TARGETS, REPLAYER_APPROVE_*)
	r, err := replayer.New()
	if err != nil {
		log.Fatalf("replayer init: %v", err)
	}

	// 3) handle Ctrl+C
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	// 4) run replayer
	if err := r.Run(ctx, forkURL); err != nil && err != context.Canceled {
		log.Fatalf("replayer run: %v", err)
	}
}
