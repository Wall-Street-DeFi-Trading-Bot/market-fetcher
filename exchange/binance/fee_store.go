package binance

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	pb "github.com/sujine/market-fetcher/proto"
	"github.com/sujine/market-fetcher/publisher"
)

var qEscaper = regexp.MustCompile(`%20|\+`)

// ---------- FeeStore: refresh spot/perp fees via REST polling & publish ----------
type FeeStore struct {
	Symbol string
	Pub    publisher.Nats

	// Use the same key for spot and futures (requires Futures READ permissions and IP allowlist)
	APIKey       string
	APISecret    string
	rawAPIKey    string
	rawAPISecret string
	debugSig     bool

	httpc *http.Client

	mu        sync.RWMutex
	spotMaker float64
	spotTaker float64
	perpMaker float64
	perpTaker float64

	_timeOffsetMs int64
}

func NewFeeStore(symbol string, pub publisher.Nats) *FeeStore {
	rawKey := os.Getenv("BINANCE_API_KEY")
	rawSecret := os.Getenv("BINANCE_API_SECRET")
	cleanKey := cleanEnv(rawKey)
	secretClean := cleanSecret(rawSecret)
	// debug := strings.TrimSpace(os.Getenv("BINANCE_DEBUG_SIG")) != ""

	fs := &FeeStore{
		Symbol:       symbol,
		Pub:          pub,
		APIKey:       cleanKey,
		APISecret:    secretClean,
		rawAPIKey:    rawKey,
		rawAPISecret: rawSecret,
		debugSig:     false,
		httpc:        &http.Client{Timeout: 10 * time.Second},
	}

	if fs.debugSig {
		fmt.Printf("[fee_store:%s] debug logging ON (BINANCE_DEBUG_SIG)\n", symbol)
		fmt.Printf("[fee_store:%s] raw key len=%d clean len=%d tail=%s\n",
			symbol, len(rawKey), len(cleanKey), hexSnippet(rawKey))
		fmt.Printf("[fee_store:%s] raw secret len=%d clean len=%d head/tail=%s\n",
			symbol, len(rawSecret), len(secretClean), hexSnippet(rawSecret))
	}

	// Measure server time offset on the initial run
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	fsOffset := fs.syncServerTime(ctx)
	cancel()
	// Cache the offset
	fs.mu.Lock()
	fs._timeOffsetMs = fsOffset
	fs.mu.Unlock()
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] server time offset=%dms\n", symbol, fsOffset)
	}
	return fs
}

// Start: most trading bots poll every 30-120 seconds; default to 60s.
func (fs *FeeStore) Start(ctx context.Context, pollEvery time.Duration) {
	if pollEvery <= 0 {
		pollEvery = 60 * time.Second
	}
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] start polling every %s\n", fs.Symbol, pollEvery)
	}
	// Run once immediately, then poll on the interval
	go fs.loopSpot(ctx, pollEvery)
	go fs.loopPerp(ctx, pollEvery)
}

func (fs *FeeStore) loopSpot(ctx context.Context, every time.Duration) {
	// Run once immediately
	fs.fetchAndPublishSpot(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fs.fetchAndPublishSpot(ctx)
		}
	}
}

func (fs *FeeStore) loopPerp(ctx context.Context, every time.Duration) {
	// Run once immediately
	fs.fetchAndPublishPerp(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fs.fetchAndPublishPerp(ctx)
		}
	}
}

func (fs *FeeStore) fetchAndPublishSpot(ctx context.Context) {
	maker, taker, err := fs.fetchSpotFee(ctx, fs.Symbol)
	if err != nil {
		// Ignore transient network errors and keep the previous values
		if fs.debugSig {
			fmt.Printf("[fee_store:%s] spot fee fetch err: %v\n", fs.Symbol, err)
		}
		return
	}
	fs.mu.Lock()
	fs.spotMaker, fs.spotTaker = maker, taker
	fs.mu.Unlock()
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] spot fee update maker=%.8f taker=%.8f\n", fs.Symbol, maker, taker)
	}

	evt := publisher.NewMarketDataBuilder(
		"Binance", fs.Symbol, pb.Venue_VENUE_CEX, pb.Instrument_INSTRUMENT_SPOT,
	).WithFee(maker, taker).Build()
	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.TsNs = time.Now().UnixNano()
	_ = fs.Pub.Publish(evt)
}

func (fs *FeeStore) fetchAndPublishPerp(ctx context.Context) {
	maker, taker, err := fs.fetchPerpFee(ctx, fs.Symbol)
	if err != nil {
		if fs.debugSig {
			fmt.Printf("[fee_store:%s] perp fee fetch err: %v\n", fs.Symbol, err)
		}
		return
	}
	fs.mu.Lock()
	fs.perpMaker, fs.perpTaker = maker, taker
	fs.mu.Unlock()
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] perp fee update maker=%.8f taker=%.8f\n", fs.Symbol, maker, taker)
	}

	evt := publisher.NewMarketDataBuilder(
		"Binance", fs.Symbol, pb.Venue_VENUE_CEX, pb.Instrument_INSTRUMENT_PERPETUAL,
	).WithFee(maker, taker).Build()
	if evt.Header == nil {
		evt.Header = &pb.Header{}
	}
	evt.Header.TsNs = time.Now().UnixNano()
	_ = fs.Pub.Publish(evt)
}

// ---------- Public getters (usable for internal estimations) ----------
func (fs *FeeStore) Spot() (maker, taker float64) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.spotMaker, fs.spotTaker
}
func (fs *FeeStore) Perp() (maker, taker float64) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.perpMaker, fs.perpTaker
}

// ---------- REST calls (signature required) ----------

// Spot: GET /sapi/v1/asset/tradeFee?symbol=SYMBOL&timestamp=...&recvWindow=...
type spotFeeResp struct {
	Symbol          string `json:"symbol"`
	MakerCommission string `json:"makerCommission"`
	TakerCommission string `json:"takerCommission"`
}

func (fs *FeeStore) fetchSpotFee(ctx context.Context, symbol string) (maker, taker float64, err error) {
	endpoint := "https://api.binance.com/sapi/v1/asset/tradeFee"
	q := url.Values{}
	q.Set("symbol", symbol)
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] spot fetch begin params=%v\n", symbol, q)
	}
	// timestamp/recvWindow are set inside signedGET
	body, err := fs.signedGET(ctx, endpoint, q)
	if err != nil {
		return 0, 0, err
	}
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] spot response raw=%s\n", symbol, body)
	}

	var arr []spotFeeResp
	if e := json.Unmarshal(body, &arr); e != nil || len(arr) == 0 {
		return 0, 0, fmt.Errorf("decode spot fee: %v", e)
	}
	return atof(arr[0].MakerCommission), atof(arr[0].TakerCommission), nil
}

// Futures(USDⓈ-M): GET /fapi/v1/commissionRate?symbol=SYMBOL&timestamp=...&recvWindow=...
type perpFeeResp struct {
	Symbol              string `json:"symbol"`
	MakerCommissionRate string `json:"makerCommissionRate"`
	TakerCommissionRate string `json:"takerCommissionRate"`
}

func (fs *FeeStore) fetchPerpFee(ctx context.Context, symbol string) (maker, taker float64, err error) {
	endpoint := "https://fapi.binance.com/fapi/v1/commissionRate"
	q := url.Values{}
	q.Set("symbol", symbol)
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] perp fetch begin params=%v\n", symbol, q)
	}
	body, err := fs.signedGET(ctx, endpoint, q)
	if err != nil {
		return 0, 0, err
	}
	if fs.debugSig {
		fmt.Printf("[fee_store:%s] perp response raw=%s\n", symbol, body)
	}

	var v perpFeeResp
	if e := json.Unmarshal(body, &v); e != nil {
		return 0, 0, fmt.Errorf("decode perp fee: %v", e)
	}
	return atof(v.MakerCommissionRate), atof(v.TakerCommissionRate), nil
}

// Apply server offset when generating timestamps
func (fs *FeeStore) nowMs() int64 {
	fs.mu.RLock()
	off := fs._timeOffsetMs
	fs.mu.RUnlock()
	return time.Now().UnixMilli() + off
}

// url.Values.Encode() sorts keys and normalizes percent encoding.
// Important: the string used for signing must match the transmitted raw query.
func canonicalEncode(v url.Values) string {
	// Start with the standard Encode output
	qs := v.Encode()
	// Force whitespace to %20 to avoid '+' substitutions
	return qEscaper.ReplaceAllStringFunc(qs, func(s string) string {
		if s == "+" || s == "%20" {
			return "%20"
		}
		return s
	})
}

func (fs *FeeStore) signedGET(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	if fs.APIKey == "" || fs.APISecret == "" {
		return nil, fmt.Errorf("missing BINANCE_API_KEY / BINANCE_API_SECRET")
	}
	// Ensure timestamp/recvWindow are set consistently here
	if params.Get("timestamp") == "" {
		params.Set("timestamp", strconv.FormatInt(fs.nowMs(), 10))
	}
	if params.Get("recvWindow") == "" {
		params.Set("recvWindow", "30000")
	}

	qs := canonicalEncode(params)

	mac := hmac.New(sha256.New, []byte(fs.APISecret)) // secret is used as-is in bytes
	mac.Write([]byte(qs))
	sig := hex.EncodeToString(mac.Sum(nil))

	if fs.debugSig {
		fmt.Printf("[fee_store:%s] signedGET endpoint=%s qs=%q sig=%s secretLen=%d\n",
			fs.Symbol, endpoint, qs, sig, len(fs.APISecret))
		if fs.rawAPISecret != fs.APISecret {
			fmt.Printf("[fee_store:%s] raw secret hex=%s\n", fs.Symbol, hexSnippet(fs.rawAPISecret))
		}
		fmt.Printf("[fee_store:%s] clean secret hex=%s\n", fs.Symbol, hexSnippet(fs.APISecret))
	}

	// Build the request using the same qs (no extra parameters)
	var buf bytes.Buffer
	buf.WriteString(endpoint)
	buf.WriteByte('?')
	buf.WriteString(qs)
	buf.WriteString("&signature=")
	buf.WriteString(sig)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buf.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", fs.APIKey)

	resp, err := fs.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		if fs.debugSig {
			fmt.Printf("[fee_store:%s] HTTP %d body=%s\n", fs.Symbol, resp.StatusCode, string(b))
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return b, nil
}

// Binance expects millisecond precision; clock drift can trigger -1022/-1021
func (fs *FeeStore) syncServerTime(ctx context.Context) int64 {
	type srv struct {
		ServerTime int64 `json:"serverTime"`
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.binance.com/api/v3/time", nil)
	resp, err := fs.httpc.Do(req)
	if err != nil || resp.StatusCode/100 != 2 {
		if resp != nil {
			resp.Body.Close()
		}
		if fs.debugSig {
			fmt.Printf("[fee_store:%s] syncServerTime error: %v status=%v\n", fs.Symbol, err, respStatus(resp))
		}
		return 0
	}
	defer resp.Body.Close()
	var v srv
	b, _ := io.ReadAll(resp.Body)
	if json.Unmarshal(b, &v) != nil || v.ServerTime == 0 {
		if fs.debugSig {
			fmt.Printf("[fee_store:%s] syncServerTime decode err body=%s\n", fs.Symbol, string(b))
		}
		return 0
	}
	// Difference between local and server time (ms)
	return v.ServerTime - time.Now().UnixMilli()
}

// ---------- Small helpers ----------
func atof(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
func respStatus(resp *http.Response) string {
	if resp == nil {
		return "nil"
	}
	return resp.Status
}
