# twlab — Taiwan Market Data Platform

A self-hosted, FinLab-parity data platform for quantitative trading on the Taiwan
market: one collection pipeline feeding a `data.get("dataset:欄位")`-style research API.

## Language

### Data model

**Dataset**:
A named table of market data mirroring one FinLab dataset (e.g. `price`, `monthly_revenue`). The unit of scraping, scheduling, QA, and cataloging.
_Avoid_: table, collection, source

**Field**:
One column within a Dataset, named exactly as FinLab names it (usually Chinese, e.g. `收盤價`).
_Avoid_: column, metric

**Data Key**:
The string that addresses data through the API — `"dataset:field"` for value fields, bare `"dataset"` for Event Tables.
_Avoid_: path, identifier

**Wide Frame**:
A DataFrame indexed by date with one column per Stock ID — the shape every value-type Data Key resolves to.
_Avoid_: pivot table, matrix

**Event Table**:
A long-form Dataset of dated records (announcements, corporate actions, penalties) served as-is rather than pivoted into a Wide Frame.
_Avoid_: log, list data

**Stock ID**:
The exchange code string identifying a listed security (e.g. `2330`).
_Avoid_: symbol, ticker

**Catalog**:
The scraped FinLab field specification (`docs/finlab_catalog.json`) — 123 Datasets, 1,462 Fields with types, sizes, and history ranges. Defines what "done" means for coverage.
_Avoid_: schema doc, field list

**FinLab Parity**:
The acceptance bar for the platform: exact Data Keys and coverage matching the Catalog, such that a FinLab example strategy runs verbatim.

### Collection

**Official Source**:
The government or exchange origin a Dataset is scraped from (TWSE, TPEx, MOPS, TDCC, TAIFEX, 國發會, 央行). Every Dataset has exactly one.
_Avoid_: provider, vendor, API

**Registry**:
The declarative index of all Datasets: Official Source, Cadence, Invariants, Backfill depth.
_Avoid_: config, manifest

**Cadence**:
When a Dataset is due for update, mirroring its Official Source's publication window (daily post-market, monthly disclosure window, quarterly).
_Avoid_: schedule, frequency

**Orchestrator**:
The scheduled process that walks the Registry and updates every Dataset that is due.
_Avoid_: cron job, runner

**Backfill**:
The one-time historical load of a Dataset to the maximum depth its Official Source archives — including legacy formats.

### Quality

**Invariant**:
A per-Dataset structural expectation (schema, row count vs Trading Calendar, value ranges, key uniqueness) checked on every scrape before data is published.

**Witness**:
An independent third-party source (FinMind) used only to cross-check samples of scraped data — never as a collection source.
_Avoid_: backup source, fallback

**Quarantine**:
The state of a scraped batch that failed an Invariant: it is held out, and the API keeps serving the last good version of the Dataset.

**Trading Calendar**:
The exchange's official trading days — the baseline for row-count Invariants and date alignment.

### Semantics

**Point-in-Time Alignment**:
Dating a fundamental value by when the market could first know it, not by the period it describes. The default behavior of the API for monthly and quarterly Datasets.
_Avoid_: lag, shift

**Statutory Deadline**:
The legal disclosure deadline used as the availability date under Point-in-Time Alignment — 月營收: 10th of the following month; 季報: 5/15, 8/14, 11/14, 3/31.

**Corporate Action**:
An event that breaks price-series continuity: 除權息, 減資, 面額變更.

**Adjusted Price**:
A back-adjusted (向後調整) OHLC series derived locally from raw prices plus Corporate Actions, exposed under the `etl:adj_*` Data Keys.
