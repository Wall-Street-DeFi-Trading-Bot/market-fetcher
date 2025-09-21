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
	WETH = "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
	WoD = "0xb994882a1b9bd98A71Dd6ea5F61577c42848B0E8"
)

const (
    Dir01 = 1 // token0 -> token1
    Dir10 = 2 // token1 -> token0
)

var ADDR_TO_SYMBOL = map[string]string{
	"0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c": "WBNB",
	"0x55d398326f99059fF775485246999027B3197955": "USDT",
	"0x2170ed0880ac9a755fd29b2688956bd959f933f8": "WETH",
}

func getTokenInfoForSymbol(sym string) (t0, t1 common.Address, d0, d1 int, ok bool) {
	s := strings.TrimSpace(sym)
	switch s {
	case "ETHUSDT":
		return common.HexToAddress(WETH), common.HexToAddress(USDT), 18, 18, true
	case "WBNBUSDT", "BNBUSDT":
		return common.HexToAddress(WBNB), common.HexToAddress(USDT), 18, 18, true
	case "WoDUSDT":
		return common.HexToAddress(WoD), common.HexToAddress(USDT), 18, 18, true
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