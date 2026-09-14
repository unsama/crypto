# crypto

Large-scale historical cryptocurrency data pipeline & dataset generation.
See [crypto_historical_dataset_pipeline.md](crypto_historical_dataset_pipeline.md) for the full spec and roadmap.

## Status

**Phase 1: Ingestion Engine & Download Automation** — done.

- [x] Task 1.1 — Binance public archive harvester (`src/crypto_pipeline/ingestion/binance.py`)
- [x] Task 1.2 — Bybit & Kraken fetchers (`src/crypto_pipeline/ingestion/bybit.py`, `kraken.py`)
- [x] Task 1.3 — Resilient download manager: resume, backoff, rate limiting, caching (`src/crypto_pipeline/ingestion/download_manager.py`)

**Phase 2: Schema Normalization & High-Throughput Processing** — done.

- [x] Task 2.1 — Polars transformers mapping each exchange's raw archive format onto the unified `trades_tick` schema, reading zip/gzip members directly without extracting to disk (`src/crypto_pipeline/transform/binance.py`, `bybit.py`, `kraken.py`)
- [x] Task 2.2 — Cleaning: drop invalid rows (non-positive price/qty, null id/timestamp), deterministic sort, dedupe on `(exchange, symbol, trade_id)` (`src/crypto_pipeline/transform/schema.py`)

Known limitation: symbol normalization (`transform/symbols.py`) is a static best-effort `BASE-QUOTE` splitter, not a live lookup against each exchange's instruments/assets endpoint — it covers common quote assets and Kraken's major legacy asset codes, but exotic pairs may need the table extended.

**Phase 3: Resampling & Feature Generation** — partially done.

- [x] Task 3.1 — Tick → 1s/1m/5m OHLCV bars: OHLC, volume, trade count, buy/sell volume split, volume delta, VWAP (`src/crypto_pipeline/features/resample.py`)
- [x] Task 3.2 (price half) — Rolling realized volatility from bar closes (`src/crypto_pipeline/features/volatility.py`)
- [ ] Task 3.2 (order-book half) — Spread/mid-price/depth-at-±1%/±2% (`src/crypto_pipeline/features/orderbook.py`) is implemented and unit-tested against synthetic snapshots, but **has no real data source**: Phase 1 never built an L2 order-book harvester (only trade-tick archives), so this can't run end-to-end yet. Building that harvester would need to happen before this task can be marked done.
- [x] Task 3.3 — Market-event tagging: flash crashes/spikes, volume-surge Z-score anomalies (both bar-based, working now), and liquidity dry-ups (spread-based, blocked on the same missing order-book data) (`src/crypto_pipeline/features/events.py`)

**Phase 4: Storage Optimization & Partitioning** — done.

- [x] Task 4.1 — Hive-partitioned Parquet: `data/gold/<dataset>/exchange=.../symbol=.../year=.../month=.../data.parquet`, ZSTD level 3 compression, `merge_key`-based idempotent re-runs (`src/crypto_pipeline/storage/partition.py`). This is now the default output of `normalize` and `resample` (pass `--out` for the old flat single-file behavior instead).
- [x] Task 4.2 — DuckDB query interface reading the partition files directly, no load step (`src/crypto_pipeline/storage/query.py`), exposed via `crypto_pipeline.cli query`.

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
```

Normalized trades land under `data/gold/trades/exchange=<exchange>/symbol=<symbol>/year=<YYYY>/month=<MM>/data.parquet` (gitignored) unless `--out` is given for a flat single-file path instead.

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
```

## Tests

```powershell
pytest
```
