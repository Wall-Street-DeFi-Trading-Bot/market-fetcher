// replayer/fork.go
package replayer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

// ForkConfig holds how to start a local fork node.
type ForkConfig struct {
	Tool       string
	SourceRPC  string
	Block      uint64
	ListenURL  string
	ListenHost string
	ListenPort int
}

// StartForkFromEnv starts a fork node if FORK_AUTO_START=1.
// Returns (rpcURL, cleanup, error).
func StartForkFromEnv(ctx context.Context) (string, func(), error) {
	if os.Getenv("FORK_AUTO_START") != "1" {
		// no-op
		return "", func() {}, nil
	}

	cfg := ForkConfig{
		Tool:       getenv("FORK_TOOL", "hardhat"),                        // hardhat | anvil
		SourceRPC:  getenv("FORK_SOURCE_RPC", getenv("BSC_HTTP_URL", "")), // source L1
		ListenHost: getenv("FORK_LISTEN_HOST", "127.0.0.1"),
		ListenPort: mustAtoi(getenv("FORK_LISTEN_PORT", "8545")),
	}

	if v := os.Getenv("FORK_BLOCK"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			cfg.Block = n
		}
	}

	cfg.ListenURL = fmt.Sprintf("http://%s:%d", cfg.ListenHost, cfg.ListenPort)

	cleanup, err := startFork(ctx, cfg)
	if err != nil {
		return "", func() {}, err
	}

	return cfg.ListenURL, cleanup, nil
}

func startFork(ctx context.Context, cfg ForkConfig) (func(), error) {
	if cfg.SourceRPC == "" {
		return func() {}, fmt.Errorf("fork source RPC is empty (set FORK_SOURCE_RPC or BSC_HTTP_URL)")
	}

	var cmd *exec.Cmd

	switch cfg.Tool {
	case "hardhat":
		args := []string{
			"hardhat", "node",
			"--hostname", cfg.ListenHost,
			"--port", fmt.Sprintf("%d", cfg.ListenPort),
			"--fork", cfg.SourceRPC,
		}
		if cfg.Block > 0 {
			args = append(args, "--fork-block-number", fmt.Sprintf("%d", cfg.Block))
		}
		cmd = exec.CommandContext(ctx, "npx", args...)
	case "anvil":
		args := []string{
			"--host", cfg.ListenHost,
			"--port", fmt.Sprintf("%d", cfg.ListenPort),
			"--fork-url", cfg.SourceRPC,
		}
		if cfg.Block > 0 {
			args = append(args, "--fork-block-number", fmt.Sprintf("%d", cfg.Block))
		}
		cmd = exec.CommandContext(ctx, "anvil", args...)
	default:
		return func() {}, fmt.Errorf("unsupported FORK_TOOL=%s", cfg.Tool)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return func() {}, fmt.Errorf("start fork process: %w", err)
	}

	// wait until RPC is up
	if err := waitRPC(ctx, cfg.ListenURL, 20*time.Second); err != nil {
		_ = cmd.Process.Kill()
		return func() {}, fmt.Errorf("fork rpc not ready: %w", err)
	}

	cleanup := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
	}
	return cleanup, nil
}

func waitRPC(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for rpc at %s", url)
		}
		cl, err := rpc.DialContext(ctx, url)
		if err == nil {
			// success, close and return
			cl.Close()
			log.Printf("[replayer] fork rpc is up at %s", url)
			return nil
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func mustAtoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
