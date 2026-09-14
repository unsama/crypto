# crypto

Historical cryptocurrency data ETL pipeline. Full spec and 5-phase roadmap: `crypto_historical_dataset_pipeline.md`.

## Status

Phase 1 (ingestion) is done. See `README.md` for the up-to-date phase checklist.

## Structure

```
src/crypto_pipeline/
  cli.py                     # entrypoint: python -m crypto_pipeline.cli <binance|bybit|kraken>
  config.py                  # DATA_ROOT / BRONZE / SILVER / GOLD paths
  ingestion/
    download_manager.py      # shared resumable/retrying/rate-limited/caching downloader
    binance.py                # data.binance.vision monthly archive harvester + checksum verify
    bybit.py                  # public.bybit.com daily archive fetcher
    kraken.py                 # api.kraken.com/0/public/Trades REST pagination
tests/                        # pytest, unit-level only (no live network calls)
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
- Raw downloads land under `data/bronze/raw/<exchange>/...`; nothing under `data/` is committed except `.gitkeep`.
- Phase 2+ (normalization, resampling, storage, QA) is not implemented yet — don't assume those modules exist.
