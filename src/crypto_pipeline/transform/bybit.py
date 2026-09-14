"""Bybit raw daily execution archives -> unified trades_tick schema (Task 2.1)."""

from __future__ import annotations

import gzip
from pathlib import Path

import polars as pl

MARKET_TYPE_MAP = {
    "linear": "linear_perpetual",
    "inverse": "inverse_perpetual",
}


def read_bybit_trades(csv_gz_path: Path, symbol: str, market: str = "linear") -> pl.LazyFrame:
    """Map a raw Bybit daily execution archive onto the unified trades_tick schema.

    Uses `homeNotional`/`foreignNotional` (base/quote asset value) instead
    of the raw `size` column, since `size` means contract count for
    inverse products but base-asset quantity for linear products - the
    notional columns sidestep that distinction.
    """
    if market not in MARKET_TYPE_MAP:
        raise ValueError(f"unknown Bybit market {market!r}")

    with gzip.open(csv_gz_path, "rb") as f:
        df = pl.read_csv(f)

    lf = df.lazy().with_columns(
        (pl.col("timestamp").cast(pl.Float64) * 1_000_000).round(0).cast(pl.Int64).alias("timestamp_utc"),
        pl.col("trdMatchID").cast(pl.Utf8).alias("trade_id"),
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col("homeNotional").cast(pl.Float64).alias("quantity"),
        pl.col("foreignNotional").cast(pl.Float64).alias("quote_quantity"),
        pl.col("side").str.to_lowercase().alias("side"),
        pl.lit(False).alias("is_liquidation"),
        pl.lit("bybit").alias("exchange"),
        pl.lit(MARKET_TYPE_MAP[market]).alias("market_type"),
        pl.lit(symbol).alias("symbol"),
    )
    return lf
