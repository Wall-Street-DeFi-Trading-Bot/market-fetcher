# Market Fetcher Setup

Follow the steps below to get the collector running locally.

## 1. Install Docker Desktop

- Download and install Docker Desktop from https://www.docker.com/products/docker-desktop/.
- Launch Docker Desktop after installation.

## 2. Clone the Repository and Start Services

```bash
git clone https://github.com/Wall-Street-DeFi-Trading-Bot/market-fetcher
cd market-fetcher
docker compose up
```

What this does:

- Spins up the NATS broker and other required containers.
- After the initial `docker compose up`, you can start/stop the containers from Docker Desktop.
- If you remove the containers, rerun `docker compose up`.

## 3. Build the Server

```bash
go build -o market-fetcher-server .
```

Replace `market-fetcher-server` with any executable name you prefer.

## 4. Configure Environment Variables

Create a `.env` file in the project root. Below is a sample configuration:

```env
BINANCE_API_KEY=YOUR_API_KEY        # Enable Reading + Enable Futures
BINANCE_API_SECRET=YOUR_API_SECRET
BSC_WS_URL=wss://<YOUR_API_DOMAIN>/v2/<YOUR_API_KEY>

DEX_POOLS_WBNBUSDT=v2@0x16b9a82891338f9ba80e2d6970fdda79d1eb0dae,v3@0x36696169C63e42cd08ce11f5deeBbCeBae652050,
DEX_POOLS_WBNBCAKE=v2@0x0eD7e52944161450477ee417DE9Cd3a859b14fD0,v3@0x133B3D95bAD5405d14d53473671200e9342896BF,
DEX_POOLS_WBNBETH=v2@0x74e4716e431f45807dcf19f284c7aa99f18a4fbc,v3@0xd0e226f674bbf064f54ab47f42473ff80db98cba,
DEX_POOLS_USDCUSDT=v3@0x92b7807bF19b7DDdf89b706143896d05228f3121,v3@0x4f31Fa980a675570939B737Ebdde0471a4Be40Eb,

NATS_URL=nats://127.0.0.1:4222

# Collectors to run (comma-separated)
EXCHANGES=binance,pancakeswap  # enable both if needed
# EXCHANGES=pancakeswap
# EXCHANGES=binance

# Symbols per exchange
DEX_SYMBOLS=WBNBUSDT,WBNBETH,WBNBCAKE,USDCUSDT
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT

LOG_PUBLISH_ENABLE=false
MF_FLUSH_TIMEOUT_MS=20

BINANCE_DEBUG_SIG=false
```

Helpful resources for pool discovery:

- PancakeSwap V3 pairs: https://pancakeswap.finance/info/v3/pairs
- PancakeSwap V2 pairs: https://pancakeswap.finance/info/v2/pairs?chainName=v2&chain=bsc
- Supported address list:
  | Pair | Version | Fee | Address |
  | --------- | :-----: | :---: | --------------------------------------------- |
  | WBNB/CAKE | V2 | 0.25% | `0x0eD7e52944161450477ee417DE9Cd3a859b14fD0` |
  | | V3 | 0.25% | `0x133B3D95bAD5405d14d53473671200e9342896BF` |
  | WBNB/ETH | V2 | 0.25% | `0x74e4716e431f45807dcf19f284c7a99f18a4fbc` |
  | | V3 | 0.05% | `0xd0e226f674bbf064f54ab47f42473ff80db98cba` |
  | WBNB/USDT | V2 | 0.25% | `0x16b9a82891338f9ba80e2d6970fdda79d1eb0dae` |
  | | V3 | 0.01% | `0x36696169C63e42c0d08ce11f5deeBbCeBae652050` |
  | USDC/USDT | V3 | 0.01% | `0x92b7807bF19b7DDf898b706143896d05228f3121` |
  | | V3 | 0.05% | `0x4f31Fa980a675570939B737Ebdd e0471a4Be40Eb` |

## 5. Run the Server

```bash
./market-fetcher-server
```

Make sure Docker containers (especially NATS) are running before launching the server binary.
