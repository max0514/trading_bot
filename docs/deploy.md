# Deploying twlab

The pipeline runs as a Docker Compose stack on an always-on server or NAS:

| Service        | Role                                                                                     |
| -------------- | ---------------------------------------------------------------------------------------- |
| `mongo`        | System of record (one collection per Dataset) and the Orchestrator's run log             |
| `orchestrator` | cron inside the container: nightly `twlab.orchestrator run` (catch-up included), weekly Witness |
| `store`        | nginx serving the materialized Parquet store read-only on port 8787 for research machines |
| `dashboard`    | optional (`--profile dashboard`): the Dash app with the *twlab Pipelines* panel           |

All configuration is by environment variable (see `.env.example`); the same names
work for `docker compose`, the CLI, and notebooks.

## First start on a clean host

```bash
git clone <this repo> && cd trading_bot
cp .env.example .env          # edit TZ / ports / FINMIND_TOKEN if needed
docker compose up -d          # builds the image, starts mongo + orchestrator + store
docker compose logs -f orchestrator
```

The orchestrator container prints its schedule and the current status on start.
From now on, every night at `TWLAB_CRON_RUN` (default 22:30 Asia/Taipei — after
TWSE/TPEx post their final files at 21:32 and MOPS revenue closes at 22:00) it
runs every Dataset that is due, catching up windows missed since the last good
run (up to `TWLAB_MAX_CATCHUP` due days per Dataset). A fresh install collects
only the latest due day of each Dataset — history is loaded explicitly:

```bash
# Backfill (idempotent; re-runs skip published days, failures are logged and skipped)
docker compose exec orchestrator python -m twlab.orchestrator backfill price --from 2024-01-01
docker compose exec orchestrator python -m twlab.orchestrator backfill monthly_revenue --from 2023-01-01

# What updated last night, what is Quarantined, when each Dataset was last good
docker compose exec orchestrator python -m twlab.orchestrator status

# Manual re-run of one Dataset (same entry point as the nightly job)
docker compose exec orchestrator python -m twlab.orchestrator run --dataset price
docker compose exec orchestrator python -m twlab.pipeline price --date 2026-08-07

# Cross-check samples against the FinMind Witness now
docker compose exec orchestrator python -m twlab.orchestrator witness
```

Logs: `docker compose logs orchestrator` tails `/var/log/twlab/orchestrator.log`
and `witness.log` (kept in the `twlab_logs` volume).

## Dashboard

```bash
docker compose --profile dashboard up -d dashboard   # http://<host>:8050
```

The *Scraper Monitor* tab gains a **twlab Pipelines** card: one row per Registry
Dataset with its latest Orchestrator outcome (Published / Quarantined / Failed /
Witness alert, with the failing Invariant), a **Run** button that triggers that
Dataset through the Orchestrator's manual path, and **Run all due**. Outcomes appear
in the card's log on the next refresh. The dashboard shares the store volume, so
manual runs materialize into the same frames the `store` service publishes.

## Research machines (FinLab-style client)

```bash
pip install -r requirements.txt
export TWLAB_SERVER_URL=http://nas.local:8787      # the `store` service
# or, with the store mounted over the LAN:  export TWLAB_REMOTE_STORE=/Volumes/twlab/store
```

```python
from twlab import data
close = data.get("price:收盤價")          # cold read: fetched and cached under ~/.twlab/store
rev   = data.get("monthly_revenue:當月營收")
(rev > rev.average(3)) & (close > close.average(60))   # monthly auto-aligns to daily
```

Warm reads touch no network (freshness re-checked every `TWLAB_CACHE_TTL` seconds,
default one hour); a changed frame on the server is re-fetched on the next check;
with the server unreachable, the last-synced frames are served with a
`StalenessWarning`.

## Without Docker

```bash
python -m twlab.orchestrator run          # from cron: 30 22 * * *
python -m twlab.serve --port 8787         # publish ~/.twlab/store for research machines
```
