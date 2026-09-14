"""Task 2.1/2.2 glue: normalize a raw archive into the unified trades_tick
schema (with symbol normalization, cleaning, sorting, and dedup applied),
then write it out as Parquet.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from crypto_pipeline.config import SILVER_DIR
from crypto_pipeline.transform.binance import read_binance_trades
from crypto_pipeline.transform.bybit import read_bybit_trades
from crypto_pipeline.transform.kraken import read_kraken_trades
from crypto_pipeline.transform.schema import finalize_trades
from crypto_pipeline.transform.symbols import normalize_symbol

logger = logging.getLogger(__name__)


def normalize_binance_file(zip_path: Path, market: str, symbol: str, data_type: str = "trades") -> pl.DataFrame:
    lf = read_binance_trades(zip_path, market=market, symbol=symbol, data_type=data_type)
    lf = lf.with_columns(pl.lit(normalize_symbol(symbol, "binance")).alias("symbol"))
    return finalize_trades(lf).collect()


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
