package wsutil

import (
	"context"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Options configures Run.
type Options struct {
	Headers       http.Header
	ReadLimit     int64
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	PingInterval   time.Duration
}

// Run dials a WS URL, reads messages, and calls handle(msg). It reconnects automatically.
// It returns only when ctx is canceled.
func Run(ctx context.Context, url string, handle func([]byte), opt *Options) {
	o := Options{
		ReadLimit:      1 << 20,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		PingInterval:   30 * time.Second,
	}
	if opt != nil {
		if opt.ReadLimit > 0 { o.ReadLimit = opt.ReadLimit }
		if opt.InitialBackoff > 0 { o.InitialBackoff = opt.InitialBackoff }
		if opt.MaxBackoff > 0 { o.MaxBackoff = opt.MaxBackoff }
		if opt.PingInterval > 0 { o.PingInterval = opt.PingInterval }
		o.Headers = opt.Headers
	}

	backoff := o.InitialBackoff
	for {
		if ctx.Err() != nil { return }
		conn, _, err := websocket.DefaultDialer.Dial(url, o.Headers)
		if err != nil {
			time.Sleep(backoff)
			if backoff < o.MaxBackoff {
				backoff *= 2
				if backoff > o.MaxBackoff { backoff = o.MaxBackoff }
			}
			continue
		}
		backoff = o.InitialBackoff
		conn.SetReadLimit(o.ReadLimit)
		_ = conn.SetReadDeadline(time.Now().Add(3 * o.PingInterval))
		conn.EnableWriteCompression(true)
		conn.SetPongHandler(func(string) error {
			return conn.SetReadDeadline(time.Now().Add(3 * o.PingInterval))
		})

		done := make(chan struct{})
		go func() {
			t := time.NewTicker(o.PingInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					_ = conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
				case <-done:
					return
				case <-ctx.Done():
					return
				}
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				close(done)
				_ = conn.Close()
				break
			}
			handle(msg)
		}
	}
}
