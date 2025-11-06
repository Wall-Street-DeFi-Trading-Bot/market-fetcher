package binance

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// env to float helper
func envF(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// cleanEnv trims whitespace, removes UTF-8 BOM, and strips wrapping quotes.
func cleanEnv(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	s = strings.TrimPrefix(s, "\ufeff")
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		first := s[0]
		last := s[len(s)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			s = s[1 : len(s)-1]
		}
	}
	return strings.TrimSpace(s)
}

// hexSnippet returns the beginning and end of the string in hex for logging.
func hexSnippet(s string) string {
	if len(s) == 0 {
		return "(empty)"
	}
	const clip = 8
	b := []byte(s)
	if len(b) <= 2*clip {
		return fmt.Sprintf("% x", b)
	}
	head := fmt.Sprintf("% x", b[:clip])
	tail := fmt.Sprintf("% x", b[len(b)-clip:])
	return head + " ... " + tail
}

func cleanSecret(s string) string {
	s = strings.TrimSpace(s)
	// Strip matching quotes at both ends
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			s = s[1 : len(s)-1]
		}
	}
	// Trim trailing CRLF
	s = strings.TrimRight(s, "\r\n")
	return s
}

// Parse an env var into time.Duration (supports "45s", "1m30s", or "45"=seconds)
func readPollEveryFromEnv(envKey string, defSeconds int) time.Duration {
	v := strings.TrimSpace(os.Getenv(envKey))
	if v == "" {
		return time.Duration(defSeconds) * time.Second
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Duration(defSeconds) * time.Second
}

// Optional per-symbol override: BINANCE_FEE_POLL_<SYMBOL> takes precedence
func pollEveryFor(symbol string) time.Duration {
	baseKey := "BINANCE_FEE_POLL"
	// Existing normalizeSymbol logic handles this format already
	symKey := baseKey + "_" + strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
	if d := readPollEveryFromEnv(symKey, 0); d > 0 {
		return d
	}
	return readPollEveryFromEnv(baseKey, 30) // default 30 seconds
}
