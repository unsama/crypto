# crypto

Historical cryptocurrency data ETL pipeline. Full spec and 5-phase roadmap: `crypto_historical_dataset_pipeline.md`.

## Status

Phase 1 (ingestion), Phase 2 (normalization), and Phase 4 (partitioned storage) are done; Phase 3 (resampling & features) is partially done. See `README.md` for the up-to-date phase checklist.

## Structure

```
src/crypto_pipeline/
  cli.py                     # entrypoint: python -m crypto_pipeline.cli <binance|bybit|kraken|normalize|resample|query>
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
    resample.py                # tick trades -> OHLCV bars (Task 3.1)
    volatility.py               # rolling realized volatility from bar closes (Task 3.2, price half)
    orderbook.py                 # spread/mid-price/depth metrics (Task 3.2, order-book half - no data source yet, see below)
    events.py                    # flash-move, volume-surge, liquidity-dry-up tagging (Task 3.3)
  storage/
    partition.py                # write_hive_partitioned(): exchange/symbol/year/month Parquet layout (Task 4.1)
    query.py                     # DuckDB connect() registering a view per dataset dir (Task 4.2)
tests/                        # pytest, unit-level only (no live network calls; transform/feature/storage tests use synthetic fixtures)
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
- Phase 5 (QA/validation, sample export) is not implemented yet — don't assume those modules exist.
