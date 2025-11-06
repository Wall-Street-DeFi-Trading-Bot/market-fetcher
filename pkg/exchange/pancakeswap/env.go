package pancakeswap

import (
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

func envStr(key, def string) string { 
	if v := os.Getenv(key); v != "" { 
		return v 
	}
	return def 
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

func envPoolsForSymbolV3V2(sym string) (v3 []common.Address, v2 []common.Address) {
	key := "DEX_POOLS_" + strings.TrimSpace(sym)
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" { 
		return nil, nil 
	}

	items := strings.Split(raw, ",")
	for _, it := range items {
		it = strings.TrimSpace(it); 
		if it == "" { 
			continue 
		}

		parts := strings.Split(it, "@")
		if len(parts) == 2 {
			ver := strings.ToLower(strings.TrimSpace(parts[0]))
			addr := strings.TrimSpace(parts[1])
			if !common.IsHexAddress(addr) { 
				continue 
			}
			switch ver { 
				case "v3": 
					v3 = append(v3, common.HexToAddress(addr))
				case "v2": 
					v2 = append(v2, common.HexToAddress(addr)) 
			}
			continue
		}
		if common.IsHexAddress(it) { 
			v3 = append(v3, common.HexToAddress(it)) 
		}
	}
	return
}
