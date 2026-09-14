# crypto

Large-scale historical cryptocurrency data pipeline & dataset generation.
See [crypto_historical_dataset_pipeline.md](crypto_historical_dataset_pipeline.md) for the full spec and roadmap.

## Status

**Phase 1: Ingestion Engine & Download Automation** — done.

- [x] Task 1.1 — Binance public archive harvester (`src/crypto_pipeline/ingestion/binance.py`)
- [x] Task 1.2 — Bybit & Kraken fetchers (`src/crypto_pipeline/ingestion/bybit.py`, `kraken.py`)
- [x] Task 1.3 — Resilient download manager: resume, backoff, rate limiting, caching (`src/crypto_pipeline/ingestion/download_manager.py`)

**Phase 2: Schema Normalization & High-Throughput Processing** — done, verified against real data.

- [x] Task 2.1 — Polars transformers mapping each exchange's raw archive format onto the unified `trades_tick` schema, extracting the zip's CSV to a scratch temp file and lazily `scan_csv`-ing it so the read → transform → filter → sort → dedupe → write chain runs through polars' streaming engine end to end (`src/crypto_pipeline/transform/binance.py`, `bybit.py`, `kraken.py`)
- [x] Task 2.2 — Cleaning: drop invalid rows (non-positive price/qty, null id/timestamp), deterministic sort, dedupe on `(exchange, symbol, trade_id)` (`src/crypto_pipeline/transform/schema.py`)

**Real-scale verification (2026-09-14)**: normalized a genuine BTCUSDT January 2024 monthly archive from `data.binance.vision` — 52,549,865 trades, 3.7GB uncompressed CSV — end to end into the gold-layer lake, then resampled to 44,640 one-minute bars (31 days × 24h × 60m, zero gaps) with realized volatility and event tagging. This is what pushed the streaming fix: a naive eager `read_csv`/`collect()` OOM'd on this machine's ~4.76GB free RAM even after removing the original all-Utf8 parsing bug, so large single-archive processing goes through `transform.pipeline.normalize_binance_file_chunks()` (row-range chunks, ~5M rows each, each finalized independently and written as its own `part-N.parquet` file via `storage.partition.write_hive_partitioned(..., part_name=...)` rather than the merge-and-rewrite `data.parquet` path, which would otherwise re-read everything written by prior chunks on every call). `resample`'s trade-reading path was fixed the same way — `pl.scan_parquet` + streaming collect, never materializing the full trades table before aggregating down to bars.

Known limitation: symbol normalization (`transform/symbols.py`) is a static best-effort `BASE-QUOTE` splitter, not a live lookup against each exchange's instruments/assets endpoint — it covers common quote assets and Kraken's major legacy asset codes, but exotic pairs may need the table extended.

**Phase 3: Resampling & Feature Generation** — partially done.

- [x] Task 3.1 — Tick → 1s/1m/5m OHLCV bars: OHLC, volume, trade count, buy/sell volume split, volume delta, VWAP (`src/crypto_pipeline/features/resample.py`)
- [x] Task 3.2 (price half) — Rolling realized volatility from bar closes (`src/crypto_pipeline/features/volatility.py`)
- [x] Task 3.2 (depth-by-percentage) — `bid_depth_usd_Npct`/`ask_depth_usd_Npct` from Binance's futures `bookDepth` archive (`transform/binance_bookdepth.py`, `features/orderbook.pivot_percentage_depth`). See "Order-book data: what's real and what isn't" below - this is real depth data, but no mid-price/spread.
- [ ] Task 3.2 (raw per-level BBO/spread) — `add_book_metrics` in `features/orderbook.py` expects per-level `bid_price_i`/`ask_price_i` columns (spec's literal `orderbook_l2_snapshots` shape). It's implemented and unit-tested against synthetic snapshots, but **no exchange publishes that as a free bulk historical archive** - it would need a live WebSocket depth-stream recorder or a paid vendor (e.g. Tardis.dev). Not a Phase 3 gap so much as a "this data doesn't exist for free" gap.
- [x] Task 3.3 — Market-event tagging: flash crashes/spikes, volume-surge Z-score anomalies (both bar-based, working now), and liquidity dry-ups (spread-based - blocked on the same missing BBO data as above) (`src/crypto_pipeline/features/events.py`)

### Order-book data: what's real and what isn't

True historical per-level L2 order books (raw bid/ask price+size arrays, with best-bid/best-ask so spread is computable) are **not available as free bulk downloads** from Binance, Bybit, or Kraken - only live trades are published that way. The spec's `orderbook_l2_snapshots` schema (section 3.2) assumes that shape, and this pipeline has no source for it.

What *is* real: Binance's futures `bookDepth` daily archive, which publishes cumulative depth/notional in 1%-wide buckets away from the mid price. That's ingested here (`binance-bookdepth` / `normalize binance-bookdepth` CLI commands, landing in `data/gold/book_depth_pct/...`) and gives genuine `bid_depth_usd_1pct`/`bid_depth_usd_2pct`/etc. numbers. It does **not** give mid-price, spread, or spread_bps - Binance doesn't publish historical BBO in bulk either, so liquidity-dry-up detection and negative-spread checks remain unimplementable against real data until a raw-level source exists.

**One more caveat**: `transform/binance_bookdepth.py`'s column-name assumptions (`timestamp,percentage,depth,notional`) are recalled from documentation, not verified against a live download - this machine's network can't reach `data.binance.vision` (see `CLAUDE.md`). Verify against one real downloaded file before relying on this in production; the parser raises a clear error if the columns don't match rather than silently mis-parsing.

**Phase 4: Storage Optimization & Partitioning** — done.

- [x] Task 4.1 — Hive-partitioned Parquet: `data/gold/<dataset>/exchange=.../symbol=.../year=.../month=.../data.parquet`, ZSTD level 3 compression, `merge_key`-based idempotent re-runs (`src/crypto_pipeline/storage/partition.py`). This is now the default output of `normalize` and `resample` (pass `--out` for the old flat single-file behavior instead).
- [x] Task 4.2 — DuckDB query interface reading the partition files directly, no load step (`src/crypto_pipeline/storage/query.py`), exposed via `crypto_pipeline.cli query`.

**Phase 5: QA, Validation & Client Sample Packaging** — done (for trade/bar data; order-book checks share Phase 3's data-source gap).

- [x] Task 5.1 — Data quality auditor: bar-gap detection (a missing bar = a zero-volume/gap interval, since resampling only emits bars that had trades), post-storage price/quantity/timestamp invariant checks, negative-spread check for order-book data (blocked on the same missing L2 source as Phase 3), and an automated markdown summary report (`src/crypto_pipeline/qa/audit.py`, `summary.py`), exposed via `crypto_pipeline.cli report`.
- [x] Task 5.2 — 24h (configurable) verification sample exporter to CSV + Parquet for any exchange/symbol (`src/crypto_pipeline/qa/sample_export.py`), exposed via `crypto_pipeline.cli sample-export`.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

## Usage

```powershell
# Binance monthly trade archives
python -m crypto_pipeline.cli binance --market spot --symbol BTCUSDT --start 2024-01 --end 2024-03

# Bybit daily execution archives
python -m crypto_pipeline.cli bybit --symbol BTCUSD --start 2024-01-01 --end 2024-01-07

# Kraken historical trades (REST pagination)
python -m crypto_pipeline.cli kraken --pair XBTUSD --start 2024-01-01 --end 2024-01-02

# Binance futures bookDepth (percentage-bucketed order-book depth, futures/um only)
python -m crypto_pipeline.cli binance-bookdepth --symbol BTCUSDT --start 2024-01-01 --end 2024-01-07
```

Downloaded archives land under `data/bronze/raw/<exchange>/...` (gitignored).

```powershell
# Normalize a downloaded archive into the unified trades_tick schema
python -m crypto_pipeline.cli normalize binance --market spot --symbol BTCUSDT --data-type trades `
    --path data/bronze/raw/binance/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-01.zip

python -m crypto_pipeline.cli normalize bybit --symbol BTCUSDT --market linear `
    --path data/bronze/raw/bybit/BTCUSDT/BTCUSDT2024-01-01.csv.gz

python -m crypto_pipeline.cli normalize kraken --pair XBTUSD `
    --path data/bronze/raw/kraken/XBTUSD

python -m crypto_pipeline.cli normalize binance-bookdepth --symbol BTCUSDT `
    --path data/bronze/raw/binance/futures/um/daily/bookDepth/BTCUSDT/BTCUSDT-bookDepth-2024-01-01.zip
```

`normalize binance` (without `--out`) processes in memory-bounded chunks and lands trades under `data/gold/trades/exchange=<exchange>/symbol=<symbol>/year=<YYYY>/month=<MM>/part-<archive>-<NNNN>.parquet` (gitignored) - one file per ~5M-row chunk rather than a single `data.parquet`, so a full month of a high-volume pair never needs to fit in memory all at once. `query`/`report`/`resample` glob every `*.parquet` in a partition, so this is transparent to anything reading the lake back. Pass `--out` for the old flat single-file behavior instead (fine for smaller archives; not memory-bounded).

```powershell
# Resample normalized trades into OHLCV bars with realized volatility and event tags
python -m crypto_pipeline.cli resample --timeframe 1m `
    --path "data/gold/trades/exchange=binance/symbol=BTC-USDT/**/data.parquet"
```

Bars land under `data/gold/bars/exchange=.../symbol=.../year=.../month=.../data.parquet` the same way.

```powershell
# Query the partitioned lake directly with SQL (DuckDB, no load step)
python -m crypto_pipeline.cli query --sql "SELECT exchange, symbol, COUNT(*) AS n FROM trades GROUP BY 1, 2"
python -m crypto_pipeline.cli query --sql "SELECT * FROM bars WHERE is_flash_move ORDER BY bar_timestamp_utc"

# Data-quality summary report (row counts, date ranges, min/max prices, bar gaps)
python -m crypto_pipeline.cli report --out data/gold/quality_report.md

# 24h verification sample (CSV + Parquet) for one exchange/symbol
python -m crypto_pipeline.cli sample-export --exchange binance --symbol BTC-USDT `
    --start 2024-01-01 --out data/samples
```

## Tests

```powershell
pytest
```
