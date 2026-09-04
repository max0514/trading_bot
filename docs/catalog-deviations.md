# Catalog deviations

Every place where twlab's Phase 1 coverage differs from a number written in a
ticket, or from a type in the Catalog, with why. Kept so a deliberate decision
does not read later as a shortfall.

The **Catalog** — `docs/finlab_catalog.json`, 123 Datasets and 1,462 Fields
scraped from FinLab's own field specification — is the coverage spec. Epic #3
says so in its own deliverable paragraph, and CONTEXT.md defines the term the
same way. Where a ticket's prose and the Catalog disagree, the code follows
the Catalog, and `test_every_catalog_key_resolves` asserts per Dataset that
the Registry's Fields are exactly the Catalog's keys.

## Field counts: epic #3's story 2 vs the Catalog

Story 2 asks that "every Phase-1 field key from the FinLab catalog" resolve,
and then lists counts: `price` 15, `monthly_revenue` 8, `financial_statement`
166, `fundamental_features` 53, `price_earning_ratio` 3, `benchmark_return` 2,
三大法人 15. Four of those match the Catalog. Three do not:

| Dataset | Story 2 | Catalog | Registry |
| --- | --- | --- | --- |
| `price` | 15 | **11** | 11 |
| `benchmark_return` | 2 | **1** | 1 |
| `financial_statement` | 166 | **158** | 158 |
| `monthly_revenue` | 8 | 8 | 8 |
| `fundamental_features` | 53 | 53 | 53 |
| `price_earning_ratio` | 3 | 3 | 3 |
| `institutional_investors_trading_summary` | 15 | 15 | 15 |

Where story 2's larger counts come from is not recorded in the issue. What is
certain is that the Catalog has no Data Key for the extra Fields, so nothing
further *can* resolve: `data.get("price:漲跌價差")` has no key to resolve to,
and neither the Official Sources nor the parsers are the limit — every column
the sources publish under a Catalog name is collected. Widening any of the
three is a Catalog entry plus a mapping, not new plumbing.

Two candidates worth checking if the counts are ever reconciled upstream:
`price`'s sources also print 證券名稱, 漲跌(+/-) and 漲跌價差 (presentation
columns FinLab does not appear to publish as Fields), and TWSE publishes a
plain 發行量加權股價指數 alongside the total-return series `benchmark_return`
carries — but neither is in the Catalog scrape, so neither is assumed here.

## Types

| Data Key | Catalog | twlab | Why |
| --- | --- | --- | --- |
| `dividend_otc:權息` | `float` | `str` | TPEx's 權/息 column is text. The recorded 2026-06-01…09-02 page carries exactly `除息`, `除權` and `除權息` across all 687 rows, and the sibling `dividend_tse:權息` is typed `str` in the Catalog for the same content — so the `float` reads as FinLab's own type inference over a column it saw empty. Coercing would destroy the data. |

## Coverage

| Dataset | twlab | FinLab | Why |
| --- | --- | --- | --- |
| `security_categories` | 2,373 rows (1,362 上市 + 1,011 上櫃 from the recordings) | 3,445 | 興櫃 (emerging board) securities are excluded. The Official Source is the TWSE ISIN site, and the Registry fetches its 上市 (`strMode=2`) and 上櫃 (`strMode=4`) pages — the two markets `price` quotes. 興櫃 securities are on neither page and have no rows in any other Phase-1 Dataset, so including them would add Stock IDs nothing else covers. |
| `security_categories` | 3 columns | 1 Catalog key | Not a deviation: it is a Static Table. The Catalog addresses it with the bare key `security_categories`, and `name` / `category` / `market` are that table's columns rather than separately addressable Data Keys. |

## Placeholders

| Where | Value | Why |
| --- | --- | --- |
| `financial_statement.market` | `"MOPS"` | Every scraped batch carries a `market` column naming where its rows came from. MOPS serves 上市 and 上櫃 filings through one endpoint and its statement pages do not say which a company is, so the column names the Official Source instead of guessing. A Stock ID's listing market is in `security_categories.market`. |
