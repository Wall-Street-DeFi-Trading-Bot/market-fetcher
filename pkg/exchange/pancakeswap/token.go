package pancakeswap

import (
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const (
	DefaultBSCWSURL = "wss://bsc-rpc.publicnode.com"
	DefaultBSCHTTP  = "https://bsc-dataseed.binance.org"

	// Token addresses on BSC mainnet
	WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
	USDT = "0x55d398326f99059fF775485246999027B3197955"
	WETH = "0x2170Ed0880ac9A755fd29B2688956BD959F933F8" // ETH on BSC
	CAKE = "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82"
	USDC = "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"
	BTCB = "0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c"
	TWT  = "0x4B0F1812e5Df2A09796481Ff14017e6005508003"
	SFP  = "0xD41FDb03Ba84762dD66a0af1a6C8540FF1ba5dfb"
)

// Swap direction for a pair
const (
	DirNone uint8 = iota
	Dir01         // token0 -> token1
	Dir10         // token1 -> token0
)

// SymbolToAddress maps a token symbol to its address.
var SymbolToAddress = map[string]common.Address{
	"WBNB": common.HexToAddress(WBNB),
	"BNB":  common.HexToAddress(WBNB), // alias

	"USDT": common.HexToAddress(USDT),
	"USDC": common.HexToAddress(USDC),

	"WETH": common.HexToAddress(WETH),
	"ETH":  common.HexToAddress(WETH), // alias

	"CAKE": common.HexToAddress(CAKE),

	"BTCB": common.HexToAddress(BTCB),
	"TWT":  common.HexToAddress(TWT),
	"SFP":  common.HexToAddress(SFP),
}

// AddressToSymbol maps a lowercase hex address to a symbol.
var AddressToSymbol = func() map[string]string {
	m := make(map[string]string, len(SymbolToAddress))
	for sym, addr := range SymbolToAddress {
		m[strings.ToLower(addr.Hex())] = sym
	}
	return m
}()

// Backward compatible alias (if other code still uses this).
var ADDR_TO_SYMBOL = AddressToSymbol

type PairInfo struct {
	BaseSymbol    string
	QuoteSymbol   string
	BaseDecimals  int
	QuoteDecimals int
}

// PairBySymbol maps a pair symbol to its token symbols and decimals.
var PairBySymbol = map[string]PairInfo{

	// CAKE / USDT  (V3 0.05%)
	"CAKEUSDT": {BaseSymbol: "CAKE", QuoteSymbol: "USDT", BaseDecimals: 18, QuoteDecimals: 18},

	// CAKE / WBNB (V2 0.25%, V3 0.25%)
	"CAKEWBNB": {BaseSymbol: "CAKE", QuoteSymbol: "WBNB", BaseDecimals: 18, QuoteDecimals: 18},

	// TWT / WBNB (V2 0.25%, V3 0.25%)
	"TWTWBNB": {BaseSymbol: "TWT", QuoteSymbol: "WBNB", BaseDecimals: 18, QuoteDecimals: 18},

	// USDT / WBNB (V2 0.25%, V3 0.05%)
	"USDTWBNB": {BaseSymbol: "USDT", QuoteSymbol: "WBNB", BaseDecimals: 18, QuoteDecimals: 18},

	// USDT / BTCB (V2 0.25%, V3 0.05%)
	"USDTBTCB": {BaseSymbol: "USDT", QuoteSymbol: "BTCB", BaseDecimals: 18, QuoteDecimals: 18},

	// SFP / WBNB
	"SFPWBNB": {BaseSymbol: "SFP", QuoteSymbol: "WBNB", BaseDecimals: 18, QuoteDecimals: 18},
}

// GetTokenInfoForSymbol returns token0, token1 addresses and decimals for a pair symbol.
func GetTokenInfoForSymbol(sym string) (t0, t1 common.Address, d0, d1 int, ok bool) {
	key := strings.ToUpper(strings.TrimSpace(sym))
	if key == "" {
		return common.Address{}, common.Address{}, 0, 0, false
	}

	pair, exists := PairBySymbol[key]
	if !exists {
		return common.Address{}, common.Address{}, 0, 0, false
	}

	a0, ok0 := SymbolToAddress[pair.BaseSymbol]
	a1, ok1 := SymbolToAddress[pair.QuoteSymbol]
	if !ok0 || !ok1 {
		return common.Address{}, common.Address{}, 0, 0, false
	}

	return a0, a1, pair.BaseDecimals, pair.QuoteDecimals, true
}

// detectDirV3 determines swap direction for v3 swaps from signed amounts.
func detectDirV3(ps *swapEvent) uint8 {
	if ps == nil {
		return DirNone
	}
	if ps.amount0 != nil && ps.amount0.Sign() > 0 && ps.amount1 != nil && ps.amount1.Sign() < 0 {
		return Dir01
	}
	if ps.amount1 != nil && ps.amount1.Sign() > 0 && ps.amount0 != nil && ps.amount0.Sign() < 0 {
		return Dir10
	}
	return DirNone
}

// detectDirV2 determines swap direction for v2 swaps from in/out amounts.
func detectDirV2(ps *swapEvent) uint8 {
	if ps == nil {
		return DirNone
	}
	if ps.a0In != nil && ps.a0In.Sign() > 0 && ps.a1Out != nil && ps.a1Out.Sign() > 0 {
		return Dir01
	}
	if ps.a1In != nil && ps.a1In.Sign() > 0 && ps.a0Out != nil && ps.a0Out.Sign() > 0 {
		return Dir10
	}
	return DirNone
}
