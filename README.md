# trading_bot

A Taiwan-stock-market trading research platform: scrapers that pull market data
into MongoDB, a vectorized backtest engine, and a Dash web app for monitoring
and exploration.

The project runs on local MongoDB by default — no
cloud account required. Pointing it at a hosted MongoDB Atlas cluster is one
environment variable away.

---

## What's in here

| Layer | Purpose | Key files |
|---|---|---|
| **Scrapers** | Pull prices / monthly revenue / quarterly financials / news / PTT into MongoDB | `scraper_in_pys/` |
| **Backtest engine** | Vectorized portfolio simulator (entries, exits, fees, metrics) | `backtest/` |
| **Strategies** | Concrete strategies on top of the engine | `stock_strategies/` |
| **Dashboard** | Dash web app for monitoring scrapers & exploring data | `dashboard/`, `run_dashboard.py` |
| **Populate helper** | One-shot script to seed MongoDB from FinMind | `populate_local_db.py` |
| **twlab** (new) | Self-hosted, FinLab-parity TW data platform — `data.get("price:收盤價")` | `twlab/`, `CONTEXT.md`, `docs/adr/` |

### twlab (Phase 1 complete)

FinLab-compatible data API backed by official-source scrapers (TWSE, TPEx, MOPS,
ISIN), MongoDB as system of record, QA Invariants + a FinMind Witness, and
materialized Parquet Wide Frames — 14 Datasets covering price, adjusted prices,
monthly revenue, quarterly statements (158 Fields), 53 財務指標, valuation ratios,
三大法人, corporate actions, security categories, and the total-return benchmark:

```python
from twlab import data
close = data.get("price:收盤價")                    # daily Wide Frame
rev_yoy = data.get("monthly_revenue:去年同月增減(%)")  # indexed by Statutory Deadline
position = (rev_yoy > 20) & (close > close.average(60))   # auto-aligns monthly → daily
data.search("營收")                                  # discover Data Keys
```

Run the nightly Orchestrator (due-ness from each Dataset's Cadence, catch-up,
Quarantine on Invariant failure), one Dataset, or a backfill:

```bash
python -m twlab.orchestrator run          # everything due; `status` shows outcomes
python -m twlab.pipeline price --date 2026-08-07
python -m twlab.orchestrator backfill monthly_revenue --from 2023-01-01
```

Deploy with `docker compose up -d` (see `docs/deploy.md`); research machines set
`TWLAB_SERVER_URL` and read through an offline-capable local cache. A FinLab
strategy runs with only its imports changed — see `examples/finlab_peg_strategy.py`
— and the legacy `backtest/data.py` loaders are a shim over twlab.

Tests (`pip install -r requirements-dev.txt`, then `pytest`) run fully offline
against recorded fixtures. Vocabulary in `CONTEXT.md`; decisions in `docs/adr/`;
where coverage or a type differs from the Catalog on purpose,
`docs/catalog-deviations.md`.

---

## System architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                            EXTERNAL DATA SOURCES                           │
│                                                                            │
│   FinMind API (FINMIND_API_KEY)         Web sources (news, PTT)            │
│   ├─ taiwan_stock_daily                 ├─ udn / cnyes / liberty           │
│   ├─ taiwan_stock_month_revenue         └─ ptt.cc/Stock                    │
│   ├─ taiwan_stock_financial_statement                                      │
│   ├─ taiwan_stock_balance_sheet                                            │
│   └─ taiwan_stock_cash_flows_statement                                     │
└────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  │  scraper_in_pys/*.py
                                  │  (lazy-init, soft-fail on DB outage)
                                  ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                MongoDB    MONGODB_URI (default: mongodb://127.0.0.1:27017) │
│                                                                            │
│   trading_bot.stock_price            trading_bot.financial_statement       │
│   trading_bot.month_revenue          trading_bot.balance_sheet             │
│   trading_bot.news                   trading_bot.cash_flow                 │
│   trading_bot.ptt_posts                                                    │
└────────────────────────────────────────────────────────────────────────────┘
                  │                                       │
                  │ backtest/data.py                      │ dashboard/app.py
                  │ (pivots long → wide DataFrames)       │
                  ▼                                       ▼
┌────────────────────────────────┐       ┌────────────────────────────────┐
│  STRATEGIES                    │       │  DASH WEB APP                  │
│  (stock_strategies/*.py)       │       │  (http://localhost:8050)       │
│                                │       │                                │
│  build entry / exit DataFrames │       │  ▸ Scraper Monitor             │
│            │                   │       │  ▸ Data Explorer               │
│            ▼                   │       │  ▸ News & Sentiment            │
│   backtest/engine.run_backtest │       │                                │
└────────────────────────────────┘       └────────────────────────────────┘
                  │
                  ▼
            Report ──► equity curve, trades, CAGR/Sharpe/MDD
```

---

## Setup

### 1. Python dependencies

```bash
pip install -r requirements.txt
```

### 2. MongoDB — pick one

**Local (recommended for development):**

```bash
# Ubuntu (one time)
wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [ arch=amd64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] \
  https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt-get update && sudo apt-get install -y mongodb-org

# Start (no systemd — fork as a daemon)
sudo mkdir -p /var/lib/mongodb /var/log/mongodb
sudo mongod --dbpath /var/lib/mongodb --logpath /var/log/mongodb/mongod.log \
  --bind_ip 127.0.0.1 --port 27017 --fork
```

**Cloud (MongoDB Atlas, optional):** set `MONGODB_URI` to your Atlas
connection string — that's it.

### 3. Environment variables

Create `.env` in the repo root:

```env
# MongoDB — defaults to local if omitted entirely
MONGODB_URI=mongodb://127.0.0.1:27017
MONGODB_DB=trading_bot

# FinMind (free key at https://finmindtrade.com/)
FINMIND_API_KEY=eyJhbGc...your_token...
```

Both keys are optional:
- No `MONGODB_URI` → falls back to `mongodb://127.0.0.1:27017`.
- No `FINMIND_API_KEY` → FinMind anonymous tier (lower rate limit).
- DB unreachable at runtime → soft-fail, dashboard stays up with a warning
  banner; scraper actions become no-ops.

---

## Usage

### Populate the database

The included helper seeds a tech-stock subset for testing. Edit
`populate_local_db.py` to change the universe and date range, or use the
scraper classes directly from your own code:

```bash
python populate_local_db.py        # all three: prices, monthly revenue, financials
python populate_local_db.py price  # just prices
python populate_local_db.py rev    # just monthly revenue
python populate_local_db.py fin    # just income / balance / cash flow
```

For the full TWSE universe (~700 tickers), call the scrapers without a
`stock_id_list`:

```python
from scraper_in_pys.stock_price import StockPriceScraper
from scraper_in_pys.monthly_revenue import MonthlyRevenueScraper
from scraper_in_pys.quarter_report import QuarterlyReportScraper

StockPriceScraper().update_data()
MonthlyRevenueScraper().update_monthly_revenue()
QuarterlyReportScraper().update_financial_statements()
```

### Run a backtest

```bash
python stock_strategies/rd_strong_short_term.py
```

Output: console summary + `rd_strong_equity.csv` / `rd_strong_trades.csv`
in the repo root.

### Build your own strategy

Three primitives — entry signal, exit signal, and a price frame — feed the
engine.

```python
from backtest.data import get_price
from backtest.engine import run_backtest

close = get_price("close")
open_ = get_price("open")

# Toy: buy 60-day high, exit when below 20-day MA for 2 days, rebalance weekly.
entry = close == close.rolling(60).max()
ma20 = close.rolling(20).mean()
below = close < ma20
exit_signal = below & below.shift(1).fillna(False)

report = run_backtest(
    entry=entry,
    exit_signal=exit_signal,
    open_price=open_,
    close_price=close,
    resample="W",
    name="60d_high_breakout",
)
print(report.summary())
```

### Start the dashboard

```bash
python run_dashboard.py
# open http://localhost:8050
```

Three tabs:

- **Scraper Monitor** — buttons to trigger each scraper, live progress, log feed.
- **Data Explorer** — pick a stock ID + data type, view candlestick / time series + raw table.
- **News & Sentiment** — latest scraped articles and PTT-board posts.

A status banner at the top reports MongoDB connectivity in real time. If the
DB is down the dashboard still loads — data tabs just stay empty.

---

## Backtest engine reference

`backtest.engine.run_backtest()`:

| Arg | Description |
|---|---|
| `entry` | bool DataFrame (date × stock) — True where entry condition holds |
| `exit_signal` | bool DataFrame — True triggers same-day exit (fills next open) |
| `open_price` | float DataFrame — trade-fill prices |
| `close_price` | float DataFrame — valuation + signal frame |
| `resample` | rebalance frequency string (`"W"`, `"2W"`, `"M"`, `"Q"`, …) |
| `fee_ratio` | one-way commission (default `0.001425`) |
| `tax_ratio` | sell-side tax (default `0.003`) |
| `initial_capital` | starting cash (default 1,000,000) |

Returns a `Report` with:

- `equity` — pd.Series, daily portfolio value
- `positions` — DataFrame, bool, end-of-day holdings
- `trades` — DataFrame: entry/exit dates + prices + net return
- `metrics` — total_return, CAGR, max_drawdown, Sharpe, Sortino, Calmar,
  win_rate, avg_trade_return
- `summary()` → printable string

---

## Known limitations

- **R&D ratio is proxied.** FinMind's free `taiwan_stock_financial_statement`
  returns only ~17 aggregate types (`Revenue`, `OperatingExpenses`,
  `GrossProfit`, …) — R&D is not broken out. `rd_strong_short_term.py` uses
  `OperatingExpenses / Revenue` as a stand-in. To get true R&D you'd either
  upgrade to a paid data tier or rebuild a MOPS scraper against TWSE's new
  endpoints under `mopsov.twse.com.tw/`.
- **No live trading.** This is a research and backtest stack — strategies
  produce historical metrics, not orders.
- **Universe is whatever's in `stock_price`.** The strategy reads
  `backtest.data.list_stocks()` which is just the set of `stock_id` values in
  the price collection. Populate more stocks → backtests see more stocks
  automatically.

---

## Project layout

```
trading_bot/
├── backtest/                            # vectorized backtest engine
│   ├── data.py                          #   long→wide loaders from MongoDB
│   └── engine.py                        #   run_backtest() + Report
├── scraper_in_pys/                      # data ingestion
│   ├── mongo.py                         #   MongoDB wrapper (local-default, soft-fail)
│   ├── stock_price.py                   #   FinMind daily OHLCV
│   ├── monthly_revenue.py               #   FinMind monthly revenue
│   ├── quarter_report.py                #   FinMind quarterly statements
│   ├── news_scraper.py                  #   udn / cnyes / liberty
│   ├── ptt_scraper.py                   #   PTT Stock board
│   └── scraper_manager.py               #   thread orchestrator for the dashboard
├── stock_strategies/                    # concrete strategies
│   └── rd_strong_short_term.py
├── dashboard/                           # Dash web app
│   ├── app.py
│   └── assets/                          #   CSS
├── populate_local_db.py                 # seed script (FinMind → MongoDB)
├── run_dashboard.py                     # dashboard entry point
└── requirements.txt
```
