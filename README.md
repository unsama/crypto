# crypto

Large-scale historical cryptocurrency data pipeline & dataset generation.
See [crypto_historical_dataset_pipeline.md](crypto_historical_dataset_pipeline.md) for the full spec and roadmap.

## Status

**Phase 1: Ingestion Engine & Download Automation** — in progress.

- [x] Task 1.1 — Binance public archive harvester (`src/crypto_pipeline/ingestion/binance.py`)
- [x] Task 1.2 — Bybit & Kraken fetchers (`src/crypto_pipeline/ingestion/bybit.py`, `kraken.py`)
- [x] Task 1.3 — Resilient download manager: resume, backoff, rate limiting, caching (`src/crypto_pipeline/ingestion/download_manager.py`)

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

## Tests

```powershell
pytest
```
