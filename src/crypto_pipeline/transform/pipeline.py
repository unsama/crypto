"""Task 2.1/2.2 glue: normalize a raw archive into the unified trades_tick
schema (with symbol normalization, cleaning, sorting, and dedup applied),
then write it out as Parquet.
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path

import polars as pl

from crypto_pipeline.config import SILVER_DIR
from crypto_pipeline.transform.binance import extract_csv_member, read_binance_trades
from crypto_pipeline.transform.bybit import read_bybit_trades
from crypto_pipeline.transform.kraken import read_kraken_trades
from crypto_pipeline.transform.schema import finalize_trades
from crypto_pipeline.transform.symbols import normalize_symbol

logger = logging.getLogger(__name__)


def normalize_binance_file(zip_path: Path, market: str, symbol: str, data_type: str = "trades") -> pl.DataFrame:
    """Extracts the archive's CSV to a scratch temp dir so the read -> transform
    -> collect chain can run through polars' streaming engine (see
    `transform.binance._read_archive_csv`'s docstring) - the temp file is
    cleaned up automatically once `collect()` finishes.

    For a small-to-medium archive this is fine, but a full month of a
    high-volume pair (BTCUSDT spot trades: ~52M rows / ~3.7GB
    uncompressed) can still exceed available memory in one eager
    collect - use `normalize_binance_file_chunks` for those.
    """
    with tempfile.TemporaryDirectory(prefix="crypto_pipeline_") as tmp_dir:
        csv_path = extract_csv_member(zip_path, Path(tmp_dir))
        lf = read_binance_trades(csv_path, market=market, symbol=symbol, data_type=data_type)
        lf = lf.with_columns(pl.lit(normalize_symbol(symbol, "binance")).alias("symbol"))
        return finalize_trades(lf).collect(engine="streaming")


def normalize_binance_file_chunks(
    zip_path: Path,
    market: str,
    symbol: str,
    data_type: str = "trades",
    chunk_rows: int = 5_000_000,
) -> Iterator[pl.DataFrame]:
    """Yields finalized ~`chunk_rows`-row DataFrames for one archive, for
    memory-bounded processing of very large monthly files that OOM under
    `normalize_binance_file`'s single eager collect.

    Each chunk is sorted/deduped independently rather than globally -
    safe because native Binance trade ids are unique within a single
    source archive, so there are no cross-chunk duplicates for the dedup
    step to catch. Pair with `storage.partition.write_hive_partitioned`'s
    `part_name` to write each chunk as its own file without re-reading
    everything written by prior chunks.

    CSV has no random access, so each chunk re-scans from the start of
    the file up to its slice - roughly ~(n_chunks+1)/2x total re-reads
    across the whole file. Worth it for correctness within tight memory
    on a one-time historical backfill; not a hot path.
    """
    with tempfile.TemporaryDirectory(prefix="crypto_pipeline_") as tmp_dir:
        csv_path = extract_csv_member(zip_path, Path(tmp_dir))

        total_rows = (
            read_binance_trades(csv_path, market=market, symbol=symbol, data_type=data_type)
            .select(pl.len())
            .collect(engine="streaming")
            .item()
        )

        offset = 0
        while offset < total_rows:
            lf = read_binance_trades(csv_path, market=market, symbol=symbol, data_type=data_type)
            lf = lf.slice(offset, chunk_rows)
            lf = lf.with_columns(pl.lit(normalize_symbol(symbol, "binance")).alias("symbol"))
            yield finalize_trades(lf).collect(engine="streaming")
            offset += chunk_rows


def normalize_bybit_file(csv_gz_path: Path, symbol: str, market: str = "linear") -> pl.DataFrame:
    lf = read_bybit_trades(csv_gz_path, symbol=symbol, market=market)
    lf = lf.with_columns(pl.lit(normalize_symbol(symbol, "bybit")).alias("symbol"))
    return finalize_trades(lf).collect()


def normalize_kraken_pair(pair_dir: Path, pair: str) -> pl.DataFrame:
    lf = read_kraken_trades(pair_dir, symbol=pair)
    lf = lf.with_columns(pl.lit(normalize_symbol(pair, "kraken")).alias("symbol"))
    return finalize_trades(lf).collect()


def write_normalized(df: pl.DataFrame, dest_path: Path) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest_path, compression="zstd", compression_level=3)
    logger.info("wrote %d row(s) -> %s", df.height, dest_path)
    return dest_path


def default_dest_path(exchange: str, symbol: str, source_path: Path) -> Path:
    stem = source_path.name.split(".")[0]
    return SILVER_DIR / "trades" / exchange / symbol / f"{stem}.parquet"
