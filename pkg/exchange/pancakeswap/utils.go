package pancakeswap

import (
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
)

func intToFloat(i *big.Int, dec int) float64 {
	if i == nil {
		return 0
	}
	bf := new(big.Float).SetInt(i)
	if dec > 0 {
		scale := new(big.Float).SetFloat64(math.Pow10(dec))
		bf.Quo(bf, scale)
	}
	out, _ := bf.Float64()
	return out
}

func absFloat(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func toStr(x *big.Int) string {
	if x == nil {
		return "0"
	}
	return x.String()
}
func zeroOr(x *big.Int) *big.Int {
	if x == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(x)
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
