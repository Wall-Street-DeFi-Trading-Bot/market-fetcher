package pancakeswap

import (
	"math"
	"math/big"
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