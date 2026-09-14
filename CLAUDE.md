# crypto

Historical cryptocurrency data ETL pipeline. Full spec and 5-phase roadmap: `crypto_historical_dataset_pipeline.md`.

## Status

Phase 1 (ingestion) and Phase 2 (normalization) are done. See `README.md` for the up-to-date phase checklist.

## Structure

```
src/crypto_pipeline/
  cli.py                     # entrypoint: python -m crypto_pipeline.cli <binance|bybit|kraken|normalize>
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
    pipeline.py                # normalize_*_file() + write_normalized() glue used by the CLI
tests/                        # pytest, unit-level only (no live network calls; transform tests use synthetic fixtures)
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
- Raw downloads land under `data/bronze/raw/<exchange>/...`; normalized output lands under `data/silver/trades/<exchange>/<symbol>/...`; nothing under `data/` is committed except `.gitkeep`.
- `transform/symbols.py` is a static best-effort symbol splitter, not a live exchange lookup — extend its tables rather than assuming it's complete for every pair.
- Phase 3+ (resampling/features, partitioned storage, QA) is not implemented yet — don't assume those modules exist.
