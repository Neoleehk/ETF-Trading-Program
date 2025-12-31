# HKSI Advisor

A concise, multi-market news-driven ETF recommendation and trading pipeline for US, HK, and CN markets.

## Overview
- Fetches and classifies news by sector and market.
- Computes per-market sector allocations from market-specific files.
- Generates per-market ETF recommendations using etf_map.json and market_weights.json.
- Executes per-market trades with separate budgets and turnover constraints.
- Produces daily logs and updates positions.

## Key Files
- fetch_sites.py: Collects CN/HK/US news (EastMoney and international) into market+sector files.
- run_full_system.py: Step 2 analysis. Builds sector summaries and per-market allocations; saves recommendations.
- integrate_hksi.py: Core engine. Ranks entities, builds ETF recommendations, trading and logging helpers.
- execute_trading_system.py: Step 3 trading. Runs per-market trades, logs, and position updates.
- run_pipeline.py: Unified fetch→analysis→trading runner (verbose).
- market_weights.json: Per-sector market weight config for long allocations.
- etf_map.json: Sector→ETF tickers (US/HK/CN + inverseUS).
- requirements.txt: Python dependencies.

## Quick Start (Windows)
```
# Create and activate venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Step 1: Fetch news
python fetch_sites.py

# Step 2: Analyze + recommendations (per market + overview)
python run_full_system.py

# Step 3: Per-market trading (requires output\positions.json)
# If starting fresh, ensure positions.json exists with all cash
python execute_trading_system.py
```

### positions.json (all-cash template)
```
{
  "date": "2025-12-31",
  "cash_by_market": {"US": 1000000, "HK": 1000000, "CN": 1000000},
  "positions": []
}
```

## Outputs
- Per-market recommendations: output/recommendation_US_YYYY-MM-DD.*, ..._HK_..., ..._CN_...
- Per-market trades: output/trades/trades_<US|HK|CN>_YYYY-MM-DD.*
- Daily logs: output/daily_logs/log_<US|HK|CN>_YYYY-MM-DD.txt
- Positions: output/positions.json

## Notes
- Short-biased sectors use inverseUS ETFs; add HK/CN inverse funds to etf_map.json to expand coverage.
- market_weights.json can override per-sector splits for long allocations.
- .gitignore excludes outputs and backups to keep the repo clean.
