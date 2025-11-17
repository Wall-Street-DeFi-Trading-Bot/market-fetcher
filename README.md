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
BINANCE_FEE_POLL=40                 # Binanace Fee polling time (s)

BSC_WS_URL=wss://<YOUR_API_DOMAIN>/v2/<YOUR_API_KEY>

NATS_URL=nats://127.0.0.1:4222

# Collectors to run (comma-separated)
EXCHANGES=binance,pancakeswap  # enable both if needed
# EXCHANGES=pancakeswap
# EXCHANGES=binance

# Symbols per exchange
DEX_SYMBOLS=WBNBUSDT,CAKEUSDT,CAKEWBNB,TWTWBNB,USDTWBNB,USDTBTCB,SFPWBNB
BINANCE_SYMBOLS=BTCUSDT,ETHUSDT

LOG_PUBLISH_ENABLE=false
MF_FLUSH_TIMEOUT_MS=20

BINANCE_DEBUG_SIG=false
```

Helpful resources for pool discovery:

- PancakeSwap V3 pairs: https://pancakeswap.finance/info/v3/pairs
- PancakeSwap V2 pairs: https://pancakeswap.finance/info/v2/pairs?chainName=v2&chain=bsc

Example
| Pair | Version | Fee | Address |
| --------- | :-----: | :---: | --------------------------------------------- |
| CAKE/USDT | V3 | 0.05% | `0x8ec186cD1Ad51c380Bd23fDe29f852226647616c` |
| CAKE/WBNB | V2 | 0.25% | `0x0eD7e52944161450477ee417DE9Cd3a859b14fD0` |
| | V3 | 0.25% | `0x133B3D95bAD5405d14d53473671200e9342896BF` |
| TWT/WBNB | V2 | 0.25% | `0x3DcB1787a95D2ea0Eb7d00887704EeBF0D79bb13` |
| | V3 | 0.25% | `0x8cCB4544b3030dACF3d4D71C658f04e8688e25b1` |
| USDT/WBNB | V2 | 0.25% | `0x16b9a82891338f9bA80E2D6970FddA79D1eb0daE` |
| | V3 | 0.05% | `0x36696169C63e42cd08ce11f5deeBbCeBae652050` |
| USDT/BTCB | V2 | 0.25% | `0x`|
| | V3 | 0.05% |`0x46Cf1cF8c69595804ba91dFdd8d6b960c9B0a7C4`|
| SFP/WBNB | V2 | 0.25% |`0x942b294e59a8c47a0F7F20DF105B082710F7C305`|
| | V3 | 0.25% |`0x64ebB904e169cB94e9788FcB68283B4C894ED881` |

## 5. Run the Server

```bash
./market-fetcher-server
```

Make sure Docker containers (especially NATS) are running before launching the server binary.
