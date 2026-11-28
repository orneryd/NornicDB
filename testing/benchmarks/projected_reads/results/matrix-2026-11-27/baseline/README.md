# PR #1826 baseline matrix (2026-11-27)

This folder contains the before-tuning measurements for NornicDB commit `b0332923ff6112ebde1a94959a50ea400c25f805` on the local Apple M2 Max Mac. Start with [summary.md](summary.md), then open the Northwind and mixed report linked from its four rows. The baseline is immutable evidence for the subsequent tuning pass.

Each mode was a fresh isolated database created with explicit `layout` and `projected` options. The four modes ran serially in the order in the summary. The server used the Nornic parser, graph-only search settings, no query cache, asynchronous writes, `GOGC=400`, and `GOMEMLIMIT=8GiB`. Northwind used the repository's `northwind_power` client at 48,000 products and 48,000 orders, 30 measured iterations plus five warmups for each of 14 queries. The existing `scripts/northwind_report.py` generated each `nornicdb.md` report.

The mixed phase appended 16 copies of the 272 Markdown files at `ddc59dc20:docs` with CRLF line endings. Corpus SHA-256 was `2e04ed37939d4d34d4d8695f0ddac448bb8a1933a0c6ff46b14d7e5f6d0f9b64`; it matches the historical file manifest. The [scratch mixed client](mixed-client.go.txt) adapts the repository benchmark client for macOS RSS sampling and adds seven deterministic nonempty subsets each for random narrow and wide property reads. Its source was built outside the checkout, so this baseline commit contains no product-code or benchmark-runtime changes.

The client verified every field of all 4,352 document records in each mode. The summary generator compared the database catalog options, 14 Northwind query row counts and result hashes, and eight mixed-phase result hashes across all four modes. All comparisons passed. The raw JSON receipts are next to each report.

Timing is one sequential run per mode, not a confidence interval. The mixed phases run on the same server in the reported order, so earlier queries can warm later ones. No power or system-wide `vm_stat` samples were collected. The report generator explicitly marks those sections unavailable; its storage classification and disk totals were measured after graceful shutdown.
