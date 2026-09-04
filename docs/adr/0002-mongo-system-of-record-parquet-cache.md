# MongoDB is the system of record; the API reads Parquet wide frames

Scrapers upsert raw long-form rows into MongoDB (resumable collection, dedup, and the
existing dashboard keep working). An ETL step materializes each `dataset:field` into a
Parquet wide frame (index = date, columns = stock_id), and `data.get()` reads only those
frames — no per-call pivots. The pipeline runs in a Docker Compose stack on an always-on
server/NAS; research machines fetch frames from the server and keep a local Parquet cache
with freshness metadata (FinLab's own client model), so notebooks work offline on
last-synced data. Data that fails QA is quarantined and the cache keeps serving the last
good version, which means the API layer never observes a half-written dataset.
We rejected Mongo-only (full-scan + pivot on every read), Parquet-only (manual
upsert/resume logic, dashboard rework), and DuckDB (single-writer blocks parallel scrapers).
