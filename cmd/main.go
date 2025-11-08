package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"

	"github.com/sujine/market-fetcher/pkg/exchange"
	_ "github.com/sujine/market-fetcher/pkg/exchange/binance"     // registers "binance" factory via init()
	_ "github.com/sujine/market-fetcher/pkg/exchange/pancakeswap" // registers "pancakeswapv3" factory via init()
	pub "github.com/sujine/market-fetcher/pkg/publisher/nats"
)

// ------------------------ loggers ------------------------
var (
	sysLog *log.Logger // stdout (lifecycle only)
	pubLog *log.Logger // file only (publish events)
)

func setupSystemLogger() {
	sysLog = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds)
}

func setupPublishLogger() {
	logPath := getEnv("LOG_PUBLISH_FILE", "./logs/market-publish.log")
	_ = os.MkdirAll(filepath.Dir(logPath), 0o755)

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		sysLog.Printf("[warn] cannot open publish log file (%s): %v", logPath, err)
		pubLog = log.New(io.Discard, "", log.Ldate|log.Ltime|log.Lmicroseconds)
		return
	}
	pubLog = log.New(f, "", log.Ldate|log.Ltime|log.Lmicroseconds)
}

// ----------------------------- config helpers -----------------------------

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// ----------------------------- admin protocol ----------------------------

const (
	adminReqSubject = "mf.admin.req"
	reqTimeout      = 2 * time.Second
)

type adminReq struct {
	Cmd string `json:"cmd"`
}

type adminStatus struct {
	Exchanges     []string `json:"exchanges"`
	Symbols       []string `json:"symbols"`
	NumCollectors int      `json:"num_collectors"`
	StartUnix     int64    `json:"start_unix"`
	UptimeSec     int64    `json:"uptime_sec"`
	Running       bool     `json:"running"`
	Logging       bool     `json:"logging"`
}

func (s adminStatus) String() string {
	start := time.Unix(s.StartUnix, 0).Format("2006-01-02 15:04:05")
	return fmt.Sprintf(
		"Status: running=%v, collectors=%d, exchanges=%v, symbols=%v, started=%s, uptime=%ds, logging=%v",
		s.Running, s.NumCollectors, s.Exchanges, s.Symbols, start, s.UptimeSec, s.Logging,
	)
}

// ----------------------------- process state -----------------------------

type service struct {
	nc         *nats.Conn
	pub        *pub.Publisher
	collectors []exchange.Collector
	startTime  time.Time

	sniffOn   bool
	stopSniff func()
}

func (s *service) runService(ctx context.Context, exchanges, symbols []string) error {
	for _, ex := range exchanges {
		syms := symbols
		switch strings.ToLower(ex) {
		case "pancakeswap", "pancakeswapv3":
			if dex := splitCSV(getEnv("DEX_SYMBOLS", "")); len(dex) > 0 {
				syms = dex
			}
		case "binance":
			if cex := splitCSV(getEnv("BINANCE_SYMBOLS", "")); len(cex) > 0 {
				syms = cex
			}
		}

		cs, err := exchange.NewCollectors(ex, syms, s.pub)
		if err != nil {
			return fmt.Errorf("init collectors (%s): %w", ex, err)
		}
		s.collectors = append(s.collectors, cs...)
	}

	if len(s.collectors) == 0 {
		return errors.New("no collectors configured")
	}

	if err := s.startAdminResponder(ctx, exchanges, symbols); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(len(s.collectors))
	for _, c := range s.collectors {
		c := c
		go func() {
			defer wg.Done()
			sysLog.Printf("[collector start] %s", c.Name())
			if err := c.Run(ctx); err != nil {
				sysLog.Printf("[%s] stop err=%v", c.Name(), err)
			} else {
				sysLog.Printf("[collector stop] %s", c.Name())
			}
		}()
	}

	<-ctx.Done()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		sysLog.Printf("timeout waiting for collectors")
	}
	return ctx.Err()
}

func (s *service) startAdminResponder(ctx context.Context, exchanges, symbols []string) error {
	sub, err := s.nc.Subscribe(adminReqSubject, func(msg *nats.Msg) {
		var req adminReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Respond([]byte(`{"error":"bad request"}`))
			return
		}
		switch strings.ToLower(req.Cmd) {
		case "status":
			now := time.Now()
			out := adminStatus{
				Exchanges:     exchanges,
				Symbols:       symbols,
				NumCollectors: len(s.collectors),
				StartUnix:     s.startTime.Unix(),
				UptimeSec:     int64(now.Sub(s.startTime).Seconds()),
				Running:       true,
				Logging:       s.sniffOn,
			}
			b, _ := json.Marshal(out)
			_ = msg.Respond(b)
		case "stop":
			_ = s.nc.Publish("mf.admin.stop", nil)
			_ = msg.Respond([]byte(`{"ok":true,"message":"stopping"}`))
		case "log_on":
			if !s.sniffOn {
				if stopSniffer, err := startPublishSniffer(s.nc); err != nil {
					_ = msg.Respond([]byte(`{"ok":false,"error":"sniffer error"}`))
					return
				} else {
					s.sniffOn = true
					s.stopSniff = stopSniffer
					sysLog.Printf("[admin] logging enabled")
				}
			}
			_ = msg.Respond([]byte(`{"ok":true,"logging":true}`))
		case "log_off":
			if s.sniffOn && s.stopSniff != nil {
				s.stopSniff()
				s.stopSniff = nil
				s.sniffOn = false
				sysLog.Printf("[admin] logging disabled")
			}
			_ = msg.Respond([]byte(`{"ok":true,"logging":false}`))
		case "log_status":
			_ = msg.Respond([]byte(fmt.Sprintf(`{"logging":%v}`, s.sniffOn)))
		default:
			_ = msg.Respond([]byte(`{"error":"unknown cmd"}`))
		}
	})
	if err != nil {
		return err
	}
	_ = sub.SetPendingLimits(-1, -1)
	sysLog.Printf("[admin] responder ready on %s", adminReqSubject)
	return nil
}

// ----------------------------- publish sniffer -----------------------------

func startPublishSniffer(nc *nats.Conn) (func(), error) {
	subs := make([]*nats.Subscription, 0, 3)
	handler := func(msg *nats.Msg) {
		parts := strings.Split(msg.Subject, ".")
		switch {
		case len(parts) >= 4 && parts[0] == "ticks":
			ex, sym := parts[1], parts[2]
			if len(sym) > 3 {
				sym = sym[:3] + "/" + sym[3:]
			}
			pubLog.Printf("[%s][%s] Published MarketEvent to subject %s", ex, sym, msg.Subject)
		case len(parts) >= 3 && parts[0] == "funding":
			ex, sym := parts[1], parts[2]
			if len(sym) > 3 {
				sym = sym[:3] + "/" + sym[3:]
			}
			pubLog.Printf("[%s][%s] Published Funding to subject %s", ex, sym, msg.Subject)
		case len(parts) >= 3 && parts[0] == "fees":
			ex, sym := parts[1], parts[2]
			if len(sym) > 3 {
				sym = sym[:3] + "/" + sym[3:]
			}
			pubLog.Printf("[%s][%s] Published Fee to subject %s", ex, sym, msg.Subject)
		default:
			pubLog.Printf("[publish] subject=%s size=%dB", msg.Subject, len(msg.Data))
		}
	}

	subjects := []string{"ticks.>", "funding.>", "fees.>"}
	for _, subj := range subjects {
		sub, err := nc.Subscribe(subj, handler)
		if err != nil {
			for _, s := range subs {
				_ = s.Unsubscribe()
			}
			return nil, fmt.Errorf("sniffer subscribe %s: %w", subj, err)
		}
		subs = append(subs, sub)
	}
	nc.Flush()

	cleanup := func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	}
	return cleanup, nil
}

// ----------------------------- CLI printing ------------------------------

func printHelp() {
	fmt.Println(`market-fetcher

Usage:
  market-fetcher start      # start service (collectors + admin responder)
  market-fetcher status     # print running service status via NATS
  market-fetcher stop       # request graceful shutdown via NATS
  market-fetcher help       # show this help

Environment:
  NATS_URL             (default: nats://127.0.0.1:4222)
  BINANCE_SYMBOLS      (optional: overrides SYMBOLS for Binance)
  DEX_SYMBOLS          (optional: symbols for PancakeSwap/PancakeSwapV3)
  SYMBOLS              (default: ETHUSDT)                 # include DEX symbol like WBNBUSDT,ETHUSDT
  LOG_PUBLISH_FILE     (default: ./logs/market-publish.log)
  LOG_PUBLISH_ENABLE   (default: true)                    # "false" to start with logging off
`)
}

// --------------------------------- main ----------------------------------

func main() {
	_ = godotenv.Load()
	setupSystemLogger()
	setupPublishLogger()

	var cmd string
	if len(os.Args) > 1 {
		cmd = strings.ToLower(strings.TrimSpace(os.Args[1]))
	} else {
		fmt.Print("Enter command: ")
		reader := bufio.NewReader(os.Stdin)
		line, _ := reader.ReadString('\n')
		cmd = strings.ToLower(strings.TrimSpace(line))
	}

	if cmd == "" {
		fmt.Println("no command provided, exiting…")
		return
	}

	natsURL := getEnv("NATS_URL", "nats://127.0.0.1:4222")

	switch cmd {
	case "help", "-h", "--help":
		printHelp()
		return

	case "status":
		nc, err := nats.Connect(natsURL, nats.Name("market-fetcher-cli"))
		if err != nil {
			sysLog.Printf("NATS connect error: %v", err)
			sysLog.Println("Tip: ensure NATS_URL is correct and server is reachable.")
			return
		}
		defer nc.Close()

		req := adminReq{Cmd: "status"}
		b, _ := json.Marshal(req)
		msg, err := nc.Request(adminReqSubject, b, reqTimeout)
		if err != nil {
			sysLog.Printf("status: no responders (service not running on this NATS?)")
			sysLog.Printf("NATS_URL=%s", natsURL)
			return
		}
		var st adminStatus
		if err := json.Unmarshal(msg.Data, &st); err == nil {
			fmt.Println(st.String())
		}
		fmt.Println(string(msg.Data))
		return

	case "stop":
		nc, err := nats.Connect(natsURL, nats.Name("market-fetcher-cli"))
		if err != nil {
			sysLog.Printf("NATS connect error: %v", err)
			sysLog.Println("Tip: ensure NATS_URL is correct and server is reachable.")
			return
		}
		defer nc.Close()

		req := adminReq{Cmd: "stop"}
		b, _ := json.Marshal(req)
		msg, err := nc.Request(adminReqSubject, b, reqTimeout)
		if err != nil {
			sysLog.Printf("stop: no responders (service not running on this NATS?)")
			sysLog.Printf("NATS_URL=%s", natsURL)
			return
		}
		fmt.Println(string(msg.Data))

		sub, _ := nc.SubscribeSync("mf.admin.stop")
		nc.Flush()
		if _, err := sub.NextMsg(2 * time.Second); err == nil {
			sysLog.Println("[admin] stop signal observed")
		}
		return

	case "start":
		// proceed
	default:
		printHelp()
		os.Exit(2)
	}

	// ---------------------- start service ----------------------
	exchanges := splitCSV(getEnv("EXCHANGES", "binance"))
	symbols := splitCSV(getEnv("SYMBOLS", "ETHUSDT"))

	sysLog.Printf("[boot] NATS_URL=%s EXCHANGES=%v SYMBOLS=%v", natsURL, exchanges, symbols)

	nc, err := nats.Connect(natsURL,
		nats.Name("market-fetcher-service"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(100*time.Millisecond),
	)
	if err != nil {
		sysLog.Fatalf("NATS connect error: %v", err)
	}
	defer nc.Close()

	_, _ = nc.Subscribe("mf.ping", func(m *nats.Msg) {
		resp := map[string]int64{
			"trecv_ns": time.Now().UnixNano(),
			"tsend_ns": time.Now().UnixNano(),
		}
		b, _ := json.Marshal(resp)
		_ = m.Respond(b)
	})

	publisher, err := pub.New(natsURL)
	if err != nil {
		sysLog.Fatalf("publisher error: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	_, _ = nc.Subscribe("mf.admin.stop", func(_ *nats.Msg) {
		sysLog.Println("[admin] stop requested")
		cancel()
	})

	s := &service{
		nc:        nc,
		pub:       publisher,
		startTime: time.Now(),
	}

	if strings.ToLower(getEnv("LOG_PUBLISH_ENABLE", "true")) != "false" {
		if stopSniffer, err := startPublishSniffer(nc); err != nil {
			sysLog.Printf("[warn] publish sniffer disabled: %v", err)
		} else {
			s.sniffOn = true
			s.stopSniff = stopSniffer
			sysLog.Printf("[boot] logging publish events to %s", getEnv("LOG_PUBLISH_FILE", "./logs/market-publish.log"))
		}
	}

	go func() {
		if err := s.runService(ctx, exchanges, symbols); err != nil && !errors.Is(err, context.Canceled) {
			sysLog.Printf("service stopped: %v", err)
		}
	}()

	select {
	case sig := <-sigCh:
		sysLog.Printf("signal received: %s, shutting down...", sig)
		cancel()
	case <-ctx.Done():
	}

	if s.sniffOn && s.stopSniff != nil {
		s.stopSniff()
	}

	time.Sleep(500 * time.Millisecond)
	sysLog.Printf("bye")
}
