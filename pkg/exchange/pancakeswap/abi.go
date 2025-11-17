package pancakeswap

const pool3ABIJSON = `[
  {
    "inputs": [],
    "name": "slot0",
    "outputs": [
      { "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160" },
      { "internalType": "int24",   "name": "tick",         "type": "int24" },
      { "internalType": "uint16",  "name": "observationIndex","type": "uint16" },
      { "internalType": "uint16",  "name": "observationCardinality","type": "uint16" },
      { "internalType": "uint16",  "name": "observationCardinalityNext","type": "uint16" },
      { "internalType": "uint8",   "name": "feeProtocol",  "type": "uint32" },
      { "internalType": "bool",    "name": "unlocked",     "type": "bool" }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "inputs": [
      { "internalType": "uint32[]", "name": "secondsAgos", "type": "uint32[]" }
    ],
    "name": "observe",
    "outputs": [
      { "internalType": "int56[]",   "name": "tickCumulatives",                    "type": "int56[]" },
      { "internalType": "uint160[]", "name": "secondsPerLiquidityCumulativeX128",  "type": "uint160[]" }
    ],
    "stateMutability": "view",
    "type": "function"
  },
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true,  "internalType": "address", "name": "sender",        "type": "address" },
      { "indexed": true,  "internalType": "address", "name": "recipient",     "type": "address" },
      { "indexed": false, "internalType": "int256",  "name": "amount0",       "type": "int256" },
      { "indexed": false, "internalType": "int256",  "name": "amount1",       "type": "int256" },
      { "indexed": false, "internalType": "uint160", "name": "sqrtPriceX96",  "type": "uint160" },
      { "indexed": false, "internalType": "uint128", "name": "liquidity",     "type": "uint128" },
      { "indexed": false, "internalType": "int24",   "name": "tick",          "type": "int24" },
      { "indexed": false, "internalType": "uint128", "name": "protocolFeesToken0", "type": "uint128" },
      { "indexed": false, "internalType": "uint128", "name": "protocolFeesToken1", "type": "uint128" }
    ],
    "name": "Swap",
    "type": "event"
  },
  {
    "inputs": [],
    "name": "fee",
    "outputs": [
      { "internalType": "uint24", "name": "", "type": "uint24" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`

const pool2ABIJSON = `[
  {
    "anonymous": false,
    "inputs": [
      { "indexed": true,  "internalType": "address", "name": "sender", "type": "address" },
      { "indexed": false, "internalType": "uint256",  "name": "amount0In", "type": "uint256" },
      { "indexed": false, "internalType": "uint256",  "name": "amount1In", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "amount0Out", "type": "uint256" },
      { "indexed": false, "internalType": "uint256", "name": "amount1Out", "type": "uint256" },
      { "indexed": true,  "internalType": "address", "name": "recipient", "type": "address"}
    ],
    "name": "Swap",
    "type": "event"
  }
]`

const pancakeV3QuoterV2ABI = `[
	{"inputs":[
		{"components":[
			{"internalType":"address","name":"tokenIn","type":"address"},
			{"internalType":"address","name":"tokenOut","type":"address"},
			{"internalType":"uint256","name":"amountIn","type":"uint256"},
			{"internalType":"uint24","name":"fee","type":"uint24"},
			{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"}
		],"internalType":"struct IQuoterV2.QuoteExactInputSingleParams","name":"params","type":"tuple"}],
		"name":"quoteExactInputSingle","outputs":[{"internalType":"uint256",
		"name":"amountOut","type":"uint256"},
		{"internalType":"uint160","name":"sqrtPriceX96After","type":"uint160"},
		{"internalType":"uint32","name":"initializedTicksCrossed","type":"uint32"},
		{"internalType":"uint256","name":"gasEstimate","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}
	]`

const pancakeV3FactoryABI = `[
  {"inputs":[
     {"internalType":"address","name":"tokenA","type":"address"},
     {"internalType":"address","name":"tokenB","type":"address"},
     {"internalType":"uint24","name":"fee","type":"uint24"}],
   "name":"getPool",
   "outputs":[{"internalType":"address","name":"pool","type":"address"}],
   "stateMutability":"view","type":"function"}
]`

// Multicall3 aggregate3 전용 최소 ABI
const multicallABIJSON = `[
  {
    "inputs": [
      {
        "components": [
          { "internalType": "address", "name": "target",       "type": "address" },
          { "internalType": "bool",    "name": "allowFailure", "type": "bool"    },
          { "internalType": "bytes",   "name": "callData",     "type": "bytes"   }
        ],
        "internalType": "struct Multicall3.Call3[]",
        "name": "calls",
        "type": "tuple[]"
      }
    ],
    "name": "aggregate3",
    "outputs": [
      {
        "components": [
          { "internalType": "bool",  "name": "success",    "type": "bool"  },
          { "internalType": "bytes", "name": "returnData", "type": "bytes" }
        ],
        "internalType": "struct Multicall3.Result[]",
        "name": "returnData",
        "type": "tuple[]"
      }
    ],
    "stateMutability": "payable",
    "type": "function"
  }
]`

const factoryABIJSON = `[
  {
    "inputs": [
      { "internalType": "address", "name": "tokenA", "type": "address" },
      { "internalType": "address", "name": "tokenB", "type": "address" },
      { "internalType": "uint24",  "name": "fee",    "type": "uint24" }
    ],
    "name": "getPool",
    "outputs": [
      { "internalType": "address", "name": "pool",   "type": "address" }
    ],
    "stateMutability": "view",
    "type": "function"
  }
]`

const v2PairABIJSON = `[
  {
    "anonymous": false,
    "inputs": [
      {"indexed": false, "internalType": "uint112", "name": "reserve0", "type": "uint112"},
      {"indexed": false, "internalType": "uint112", "name": "reserve1", "type": "uint112"}
    ],
    "name": "Sync", "type": "event"
  }
]`
