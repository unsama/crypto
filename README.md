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

Normalized output lands under `data/silver/trades/<exchange>/<symbol>/...parquet` (gitignored) unless `--out` is given.

## Tests

```powershell
pytest
```
