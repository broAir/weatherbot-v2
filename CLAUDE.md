# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Bot

```bash
pip install requests

python bot_v2.py           # start main trading loop
python bot_v2.py status    # show balance and open positions
python bot_v2.py report    # full breakdown of resolved markets
python bot_v2.py wallet    # live wallet balance on Polygon
```

There are no build steps, test framework, or linter configured.

## Architecture

**bot_v2.py** is the active production bot (~1,200 lines, single file). **bot_v1.py** is a simpler educational reference (US cities only, NWS forecast only, flat position sizing).

The bot trades weather temperature outcome markets on Polymarket. Every hour it scans 20 cities × 2 product types (max/min temperature), fetches forecasts, computes expected value against live market prices, and places trades when edge exceeds threshold. Every 10 minutes it monitors open positions for stop-loss/take-profit triggers.

### Main Loop

```
scan_and_update()  [every SCAN_INTERVAL=3600s]
  └─ per city/product:
       take_forecast_snapshot()  → ECMWF + HRRR/GFS + METAR
       get_polymarket_event()    → Gamma API
       parse_temp_range()        → regex on market question text
       calc_ev()                 → edge = forecast prob vs market price
       calc_kelly()              → position size
       place trade if EV > MIN_EV

monitor_positions()  [every MONITOR_INTERVAL=600s]
  └─ check stop-loss / trailing stop / take-profit → close + update balance
```

### Key Math

- `bucket_prob()` — normal CDF probability over a temperature bucket
- `calc_ev()` — `p * (1/price - 1) - (1-p)`
- `calc_kelly()` — Kelly fraction, capped at `KELLY_FRACTION` config param
- `bet_size()` — `min(kelly * balance, MAX_BET)`

Forecast accuracy is self-calibrated: `data/calibration.json` stores learned sigmas per city/source/product and is updated on market resolution.

### Data Persistence

All state is JSON files (excluded from git via `.gitignore`):

| Path | Contents |
|------|----------|
| `data/state.json` | balance, trade count, win/loss stats |
| `data/calibration.json` | learned forecast sigmas |
| `data/markets/{product}_{city}_{date}.json` | per-market snapshots, price history, position details, resolution outcome |

### External APIs

| API | Auth | Purpose |
|-----|------|---------|
| Open-Meteo | none | ECMWF + HRRR/GFS forecasts |
| Aviation Weather | none | METAR real-time observations |
| Polymarket Gamma API | none | market events and prices |
| Visual Crossing | `VISUAL_CROSSING_API_KEY` in config.json | historical temps for resolution |
| Polygon RPC | `POLYGON_RPC_URL` env var | on-chain wallet balance |

### Credentials

Live trading credentials are never in code or config — loaded from environment variables at runtime:

```
PRIVATE_KEY, CHAIN_ID, PROXY_KEY, WALLET_ADDRESS
POLY_API_KEY, POLY_SECRET, POLY_PASSPHRASE
POLYGON_RPC_URL, POLYGON_RPC_URLS
```

`scripts/setup_live_secrets.sh` stores them in macOS Keychain; `scripts/load_live_env.sh` exports them into the shell.

`config.json` holds non-secret tuning parameters: balance, EV threshold, Kelly fraction, max bet, spread limit, scan/monitor intervals, and the Visual Crossing key.
