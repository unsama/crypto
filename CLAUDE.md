# crypto

Historical cryptocurrency data ETL pipeline. Full spec and 5-phase roadmap: `crypto_historical_dataset_pipeline.md`.

## Status

Phases 1, 2, 4, and 5 are done; Phase 3 (resampling & features) is partially done - real per-level order-book BBO/spread data doesn't exist as a free bulk download from any of the three exchanges, so that part of Task 3.2 plus the liquidity-dry-up rule have no real data source (see "Order-book data" below). Binance's `bookDepth` archive (percentage-bucketed depth, not raw levels) *is* real and ingested. See `README.md` for the up-to-date phase checklist.

## Structure

```
src/crypto_pipeline/
  cli.py                     # entrypoint: python -m crypto_pipeline.cli <binance|bybit|kraken|binance-bookdepth|normalize|resample|query|report|sample-export>
  config.py                  # DATA_ROOT / BRONZE / SILVER / GOLD paths
  ingestion/
    download_manager.py      # shared resumable/retrying/rate-limited/caching downloader
    binance.py                # data.binance.vision monthly (trades/aggTrades) + daily (bookDepth) archive harvester + checksum verify
    bybit.py                  # public.bybit.com daily archive fetcher
    kraken.py                 # api.kraken.com/0/public/Trades REST pagination
  transform/
    schema.py                 # unified trades_tick schema + cleaning/dedup/sort (Task 2.2)
    symbols.py                 # best-effort exchange symbol -> BASE-QUOTE normalization
    binance.py, bybit.py, kraken.py  # raw trades archive -> unified schema parsers (Task 2.1); binance.py extracts to a temp file + scan_csv (lazy), not read_csv, for streaming
    binance_bookdepth.py       # raw bookDepth archive -> percentage-bucket depth schema (unverified against a live file, see its docstring)
    pipeline.py                # normalize_*_file() (single eager collect) + normalize_binance_file_chunks() (memory-bounded, for large archives) + write_normalized() (flat, --out path)
  features/
    resample.py                # tick trades -> OHLCV bars (Task 3.1); TIMEFRAME_EVERY/TIMEFRAME_SECONDS reused by qa/audit.py
    volatility.py               # rolling realized volatility from bar closes (Task 3.2, price half)
    orderbook.py                 # add_book_metrics(): raw-level spread/mid-price/depth (no data source, synthetic-tested only); pivot_percentage_depth(): real Binance bookDepth -> depth-at-Npct columns
    events.py                    # flash-move, volume-surge, liquidity-dry-up tagging (Task 3.3)
  storage/
    partition.py                # write_hive_partitioned(): exchange/symbol/year/month Parquet layout (Task 4.1); part_name=... writes part-<name>.parquet without merge-read-back, for chunked large-archive ingestion
    query.py                     # DuckDB connect() registering a view per dataset dir over every *.parquet file (Task 4.2)
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
- Raw downloads land under `data/bronze/raw/<exchange>/...`; normalized trades and resampled bars default to hive-partitioned `data/gold/<trades|bars>/exchange=.../symbol=.../year=.../month=.../*.parquet` (via `storage.partition.write_hive_partitioned`) unless `--out` is passed to `normalize`/`resample` for a flat single-file path instead; nothing under `data/` is committed except `.gitkeep`.
- **Memory-bounded processing of large archives**: `normalize binance` (default path, no `--out`) does NOT eagerly collect the whole file - a full month of a high-volume pair (BTCUSDT spot trades: ~52.5M rows / ~3.7GB uncompressed, confirmed real) will OOM a single eager `read_csv`/`collect()` on a memory-constrained machine even with native dtypes. It uses `transform.pipeline.normalize_binance_file_chunks()` (row-range chunks via `.slice()` on a lazy `scan_csv`, each chunk finalized independently and written as its own `part-<archive>-<NNNN>.parquet` via `write_hive_partitioned(..., part_name=...)`). `resample` reads trades back the same way - `pl.scan_parquet(paths)` (lazy), never `pl.read_parquet` + `pl.concat` (eager) - since `resample_trades_to_bars` now accepts a LazyFrame and only collects the much smaller bar output. Any new code path that reads a whole partition's trades back should do the same: scan lazily, collect only the small aggregated result.
- `pl.read_csv(..., infer_schema_length=0)` forces every column to Utf8 - looks like a reasonable way to dodge a header-detection problem, but multiplies memory use several times over on a large real file (string storage vs. int64/float64) and is what originally caused the OOM here. Peek the first line to detect a header, then read with native dtype inference instead.
- `transform/symbols.py` is a static best-effort symbol splitter, not a live exchange lookup — extend its tables rather than assuming it's complete for every pair.
- **Order-book data**: `add_book_metrics` (raw per-level bid/ask, spread, mid-price) has **no real data source** — no exchange publishes that as a free bulk historical archive, only live trades. It's implemented and tested against synthetic input only. The liquidity-dry-up rule in `features/events.py` needs that same missing spread data. The one real exception is Binance's futures `bookDepth` daily archive (percentage-bucketed cumulative depth, not raw levels, no BBO/spread) — ingested via `binance-bookdepth`/`normalize binance-bookdepth` into `data/gold/book_depth_pct/...`, pivoted to `bid_depth_usd_Npct`/`ask_depth_usd_Npct` via `pivot_percentage_depth`. `transform/binance_bookdepth.py`'s column names are recalled from documentation, **not verified against a live file** (this machine can't reach data.binance.vision) — verify against a real download before trusting it in production; it raises a clear error on an unexpected schema rather than mis-parsing silently.
- Rolling/Z-score baselines should exclude the current row (see `tag_volume_surges`'s use of `.shift(1)`) - including it dilutes exactly the anomaly you're trying to detect.
- `write_hive_partitioned`'s idempotency depends on `merge_key`: pass it (e.g. `["exchange","symbol","trade_id"]`) when a partition may receive overlapping data across separate runs (e.g. daily Bybit files landing in the same month); omit it only when each call fully owns a partition's data for that run.
- `glob.glob()` needs `recursive=True` for a `**` pattern to actually recurse - a bug in `run_resample` was caught by smoke-testing the CLI end-to-end rather than only unit tests, and is now fixed. Prefer an end-to-end CLI smoke test (not just unit tests) when changing path-globbing or CLI wiring.
- This machine's local timezone is Asia/Karachi (UTC+5), which is *not* UTC - a DuckDB `to_timestamp()` column rendered as `+05:00`-shifted local time despite being labeled "(UTC)" in the `report` command, caught by the same kind of CLI smoke test. Use `make_timestamp(<epoch_us>)` (not `to_timestamp(<epoch_s>)`) to get a naive UTC-wall-clock timestamp with no local-zone conversion. Windows also has no built-in IANA tzdata - the `tzdata` package is a dependency (Windows-only marker in pyproject.toml) so `zoneinfo`/duckdb/polars timezone lookups don't crash.
- Phase 5 (QA/validation, sample export) is done - see `qa/` above.
