package pancakeswap

import (
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const (
	DEFAULT_BSC_WS_URL = "wss://bsc-rpc.publicnode.com"
	DEFAULT_BSC_HTTP   = "https://bsc-dataseed.binance.org"

	WBNB = "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"
	USDT = "0x55d398326f99059fF775485246999027B3197955"
	WETH = "0x2170Ed0880ac9A755fd29B2688956BD959F933F8"
	CAKE = "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82"
	USDC = "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d"
)

const (
    Dir01 = 1 // token0 -> token1
    Dir10 = 2 // token1 -> token0
)

var ADDR_TO_SYMBOL = map[string]string{
	"0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c": "WBNB",
	"0x55d398326f99059fF775485246999027B3197955": "USDT",
	"0x2170Ed0880ac9A755fd29B2688956BD959F933F8": "WETH",
	"0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82": "CAKE",
	"0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d": "USDC",
}

func getTokenInfoForSymbol(sym string) (t0, t1 common.Address, d0, d1 int, ok bool) {
	s := strings.TrimSpace(sym)
	switch s {
	case "WBNBETH", "BNBETH":
		return common.HexToAddress(WBNB), common.HexToAddress(WETH), 18, 18, true
	case "WBNBUSDT", "BNBUSDT":
		return common.HexToAddress(WBNB), common.HexToAddress(USDT), 18, 18, true
	case "WBNBCAKE", "BNBCAKE":
		return common.HexToAddress(WBNB), common.HexToAddress(CAKE), 18, 18, true
	case "USDCUSDT":
		return common.HexToAddress(USDC), common.HexToAddress(USDT), 18, 18, true
	default:
		return common.Address{}, common.Address{}, 0, 0, false
	}
}

func detectDirV3(ps *swapEvent) uint8 {
    if ps.amount0 != nil && ps.amount0.Sign() > 0 && ps.amount1 != nil && ps.amount1.Sign() < 0 { return Dir01 }
    if ps.amount1 != nil && ps.amount1.Sign() > 0 && ps.amount0 != nil && ps.amount0.Sign() < 0 { return Dir10 }
    return 0
}
func detectDirV2(ps *swapEvent) uint8 {
    if ps.a0In != nil && ps.a0In.Sign() > 0 && ps.a1Out != nil && ps.a1Out.Sign() > 0 { return Dir01 }
    if ps.a1In != nil && ps.a1In.Sign() > 0 && ps.a0Out != nil && ps.a0Out.Sign() > 0 { return Dir10 }
    return 0
}