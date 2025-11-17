package pancakeswap

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
)

// BSC mainnet factory addresses
const (
	PancakeV2FactoryAddr = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
	PancakeV3FactoryAddr = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
)

// Minimal factory interfaces (match your abigen types)
type V2Factory interface {
	GetPair(opts *bind.CallOpts, tokenA, tokenB common.Address) (common.Address, error)
}

type V3Factory interface {
	GetPool(opts *bind.CallOpts, token0, token1 common.Address, fee *big.Int) (common.Address, error)
}

var (
	ErrEmptySymbol        = errors.New("empty symbol")
	ErrSymbolNotSupported = errors.New("symbol not supported by GetTokenInfoForSymbol")
	ErrNoTokens           = errors.New("token addresses not found for pair")
)

// Fee tiers you want to probe for v3 pools
var DefaultV3Fees = []*big.Int{
	big.NewInt(100),   // 0.01%
	big.NewInt(500),   // 0.05%
	big.NewInt(2500),  // 0.25%
	big.NewInt(10000), // 1%
}

// EnvSymbols parses DEX_SYMBOLS (e.g. "WBNBUSDT,WBNBETH,USDCUSDT").
func EnvSymbols() []string {
	raw := strings.TrimSpace(os.Getenv("DEX_SYMBOLS"))
	if raw == "" {
		return nil
	}
	items := strings.Split(raw, ",")
	out := make([]string, 0, len(items))
	for _, it := range items {
		it = strings.TrimSpace(it)
		if it == "" {
			continue
		}
		out = append(out, strings.ToUpper(it))
	}
	return out
}

// PoolsForSymbolV3V2 resolves v3/v2 pool addresses for a given pair symbol.
// - sym: pair symbol like "WBNBUSDT"
// - v3Factory / v2Factory: your abigen factory instances (can be nil if not used)
func PoolsForSymbolV3V2(
	ctx context.Context,
	sym string,
	v3Factory V3Factory,
	v2Factory V2Factory,
) (v3 []common.Address, v2 []common.Address, err error) {
	sym = strings.ToUpper(strings.TrimSpace(sym))
	if sym == "" {
		return nil, nil, ErrEmptySymbol
	}

	// 1) Resolve tokens from our static config
	t0, t1, _, _, ok := GetTokenInfoForSymbol(sym)
	if !ok {
		return nil, nil, ErrSymbolNotSupported
	}
	if (t0 == common.Address{}) || (t1 == common.Address{}) {
		return nil, nil, ErrNoTokens
	}

	callOpts := &bind.CallOpts{Context: ctx}

	// 2) V2 pool (order-agnostic)
	if v2Factory != nil {
		pairAddr, err := v2Factory.GetPair(callOpts, t0, t1)
		if err != nil {
			return nil, nil, err
		}
		if pairAddr != (common.Address{}) {
			v2 = append(v2, pairAddr)
		}
	}

	// 3) V3 pools (token0 < token1 by address)
	if v3Factory != nil {
		token0 := t0
		token1 := t1
		if strings.ToLower(t1.Hex()) < strings.ToLower(t0.Hex()) {
			token0, token1 = t1, t0
		}

		for _, fee := range DefaultV3Fees {
			pool, err := v3Factory.GetPool(callOpts, token0, token1, fee)
			if err != nil {
				// just skip this fee tier
				continue
			}
			if pool == (common.Address{}) {
				continue
			}
			v3 = append(v3, pool)
		}
	}

	return v3, v2, nil
}

// Minimal ABIs for Pancake factories.
const pancakeV2FactoryABIJSON = `[
  {
    "inputs": [
      { "internalType": "address", "name": "tokenA", "type": "address" },
      { "internalType": "address", "name": "tokenB", "type": "address" }
    ],
    "name": "getPair",
    "outputs": [
      { "internalType": "address", "name": "pair", "type": "address" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`

const pancakeV3FactoryABIJSON = `[
  {
    "inputs": [
      { "internalType": "address", "name": "token0", "type": "address" },
      { "internalType": "address", "name": "token1", "type": "address" },
      { "internalType": "uint24",  "name": "fee",    "type": "uint24"  }
    ],
    "name": "getPool",
    "outputs": [
      { "internalType": "address", "name": "pool", "type": "address" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`

type onChainV2Factory struct {
	contract *bind.BoundContract
}

type onChainV3Factory struct {
	contract *bind.BoundContract
}

func newOnChainV2Factory(c bind.ContractCaller) (V2Factory, error) {
	parsed, err := abi.JSON(strings.NewReader(pancakeV2FactoryABIJSON))
	if err != nil {
		return nil, err
	}
	addr := common.HexToAddress(PancakeV2FactoryAddr)
	ctr := bind.NewBoundContract(addr, parsed, c, nil, nil)
	return &onChainV2Factory{contract: ctr}, nil
}

func newOnChainV3Factory(c bind.ContractCaller) (V3Factory, error) {
	parsed, err := abi.JSON(strings.NewReader(pancakeV3FactoryABIJSON))
	if err != nil {
		return nil, err
	}
	addr := common.HexToAddress(PancakeV3FactoryAddr)
	ctr := bind.NewBoundContract(addr, parsed, c, nil, nil)
	return &onChainV3Factory{contract: ctr}, nil
}

func (f *onChainV2Factory) GetPair(opts *bind.CallOpts, tokenA, tokenB common.Address) (common.Address, error) {
	if opts == nil {
		opts = &bind.CallOpts{}
	}

	var out []interface{}
	if err := f.contract.Call(opts, &out, "getPair", tokenA, tokenB); err != nil {
		return common.Address{}, err
	}
	if len(out) == 0 {
		return common.Address{}, nil
	}

	addr, ok := out[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("getPair: unexpected type %T", out[0])
	}

	return addr, nil
}

func (f *onChainV3Factory) GetPool(opts *bind.CallOpts, token0, token1 common.Address, fee *big.Int) (common.Address, error) {
	if opts == nil {
		opts = &bind.CallOpts{}
	}

	var out []interface{}
	if err := f.contract.Call(opts, &out, "getPool", token0, token1, fee); err != nil {
		return common.Address{}, err
	}
	if len(out) == 0 {
		return common.Address{}, nil
	}

	addr, ok := out[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("getPool: unexpected type %T", out[0])
	}

	return addr, nil
}
