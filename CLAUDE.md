# crypto

Historical cryptocurrency data ETL pipeline. Full spec and 5-phase roadmap: `crypto_historical_dataset_pipeline.md`.

## Status

Phases 1, 2, 4, and 5 are done; Phase 3 (resampling & features) is partially done - order-book metrics and the liquidity-dry-up rule have no real data source (see below). See `README.md` for the up-to-date phase checklist.

## Structure

```
src/crypto_pipeline/
  cli.py                     # entrypoint: python -m crypto_pipeline.cli <binance|bybit|kraken|normalize|resample|query|report|sample-export>
  config.py                  # DATA_ROOT / BRONZE / SILVER / GOLD paths
  ingestion/
    download_manager.py      # shared resumable/retrying/rate-limited/caching downloader
    binance.py                # data.binance.vision monthly archive harvester + checksum verify
    bybit.py                  # public.bybit.com daily archive fetcher
    kraken.py                 # api.kraken.com/0/public/Trades REST pagination
  transform/
    schema.py                 # unified trades_tick schema + cleaning/dedup/sort (Task 2.2)
    symbols.py                 # best-effort exchange symbol -> BASE-QUOTE normalization
    binance.py, bybit.py, kraken.py  # raw archive -> unified schema parsers (Task 2.1)
    pipeline.py                # normalize_*_file() + write_normalized() (flat, --out path) used by the CLI
  features/
    resample.py                # tick trades -> OHLCV bars (Task 3.1); TIMEFRAME_EVERY/TIMEFRAME_SECONDS reused by qa/audit.py
    volatility.py               # rolling realized volatility from bar closes (Task 3.2, price half)
    orderbook.py                 # spread/mid-price/depth metrics (Task 3.2, order-book half - no data source yet, see below)
    events.py                    # flash-move, volume-surge, liquidity-dry-up tagging (Task 3.3)
  storage/
    partition.py                # write_hive_partitioned(): exchange/symbol/year/month Parquet layout (Task 4.1)
    query.py                     # DuckDB connect() registering a view per dataset dir (Task 4.2)
  qa/
    audit.py                     # detect_bar_gaps / detect_price_anomalies / detect_negative_spreads (Task 5.1)
    summary.py                    # generate_summary_report(): markdown report over a DuckDB connection (Task 5.1)
    sample_export.py              # export_sample(): CSV+Parquet verification slice for one exchange/symbol (Task 5.2)
tests/                        # pytest, unit-level only (no live network calls; transform/feature/storage/qa tests use synthetic fixtures)
data/{bronze,silver,gold}/    # gitignored data lake; only .gitkeep is tracked
```

## Commands

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest
python -m crypto_pipeline.cli binance --market spot --symbol BTCUSDT --start 2024-01 --end 2024-03
```

## Conventions

- Async I/O throughout ingestion (`aiohttp`/`aiofiles`); retries via `tenacity`.
- Every new exchange fetcher should route actual file downloads through `DownloadManager` rather than raw `aiohttp` calls, to keep resume/retry/caching consistent.
- Raw downloads land under `data/bronze/raw/<exchange>/...`; normalized trades and resampled bars default to hive-partitioned `data/gold/<trades|bars>/exchange=.../symbol=.../year=.../month=.../data.parquet` (via `storage.partition.write_hive_partitioned`) unless `--out` is passed to `normalize`/`resample` for a flat single-file path instead; nothing under `data/` is committed except `.gitkeep`.
- `transform/symbols.py` is a static best-effort symbol splitter, not a live exchange lookup — extend its tables rather than assuming it's complete for every pair.
- `features/orderbook.py` and the liquidity-dry-up rule in `features/events.py` have **no real data source** — this pipeline has never ingested L2 order book snapshots (Phase 1 only built trade-tick harvesters). They're implemented and tested against synthetic input so they're ready once an order-book harvester exists, but don't assume they run against real data yet.
- Rolling/Z-score baselines should exclude the current row (see `tag_volume_surges`'s use of `.shift(1)`) - including it dilutes exactly the anomaly you're trying to detect.
- `write_hive_partitioned`'s idempotency depends on `merge_key`: pass it (e.g. `["exchange","symbol","trade_id"]`) when a partition may receive overlapping data across separate runs (e.g. daily Bybit files landing in the same month); omit it only when each call fully owns a partition's data for that run.
- `glob.glob()` needs `recursive=True` for a `**` pattern to actually recurse - a bug in `run_resample` was caught by smoke-testing the CLI end-to-end rather than only unit tests, and is now fixed. Prefer an end-to-end CLI smoke test (not just unit tests) when changing path-globbing or CLI wiring.
- This machine's local timezone is Asia/Karachi (UTC+5), which is *not* UTC - a DuckDB `to_timestamp()` column rendered as `+05:00`-shifted local time despite being labeled "(UTC)" in the `report` command, caught by the same kind of CLI smoke test. Use `make_timestamp(<epoch_us>)` (not `to_timestamp(<epoch_s>)`) to get a naive UTC-wall-clock timestamp with no local-zone conversion. Windows also has no built-in IANA tzdata - the `tzdata` package is a dependency (Windows-only marker in pyproject.toml) so `zoneinfo`/duckdb/polars timezone lookups don't crash.
- Phase 5 (QA/validation, sample export) is done - see `qa/` above.
