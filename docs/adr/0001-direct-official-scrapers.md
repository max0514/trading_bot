# All datasets are scraped directly from official sources

twlab targets full FinLab parity (all 123 datasets in `docs/finlab_catalog.json`), and every
dataset is collected directly from its official source — TWSE, TPEx, MOPS, TDCC, TAIFEX,
國發會, 央行 — with backfill to each archive's maximum depth, legacy formats included.
We rejected FinMind (caps coverage at its catalog, 600 req/hr free tier), a hybrid
(two collection paths to maintain), and mirroring FinLab VIP (subscription + ToS risk).
The price is a large scraper surface to maintain; the payoff is no rate limits, no
third-party catalog ceiling, and the deepest available history. FinMind is demoted to a
**witness**: it is only used to cross-check samples of our own scrapes, never as a source.
