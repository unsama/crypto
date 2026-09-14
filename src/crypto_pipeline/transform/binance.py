"""Binance raw trade/aggTrade archives -> unified trades_tick schema (Task 2.1)."""

from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl

MARKET_TYPE_MAP = {
    "spot": "spot",
    "futures/um": "linear_perpetual",
    "futures/cm": "inverse_perpetual",
}

TRADES_COLUMNS = ["id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "is_best_match"]
AGG_TRADES_COLUMNS = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
    "is_best_match",
]


def _looks_numeric(value: str) -> bool:
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def _read_archive_csv(zip_path: Path, columns: list[str]) -> pl.DataFrame:
    """Read the single CSV member of a Binance archive zip into memory as strings.

    Binance archives are inconsistent about whether they include a header
    row, so every column is read as Utf8 (no type inference) and the
    header row, if present, is detected by checking whether the first id
    value parses as a number, then dropped. Callers cast columns to their
    real types explicitly downstream.
    """
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one .csv member in {zip_path}, found {zf.namelist()}")
        with zf.open(members[0]) as f:
            df = pl.read_csv(
                f,
                has_header=False,
                new_columns=columns,
                infer_schema_length=0,
            )

    if df.height and not _looks_numeric(df[columns[0]][0]):
        df = df.slice(1, df.height - 1)
    return df


def read_binance_trades(zip_path: Path, market: str, symbol: str, data_type: str = "trades") -> pl.LazyFrame:
    """Map a raw Binance trades/aggTrades archive onto the unified trades_tick schema."""
    if market not in MARKET_TYPE_MAP:
        raise ValueError(f"unknown Binance market {market!r}")
    market_type = MARKET_TYPE_MAP[market]

    if data_type == "trades":
        columns = TRADES_COLUMNS
        id_col, time_col, qty_col = "id", "time", "qty"
    elif data_type == "aggTrades":
        columns = AGG_TRADES_COLUMNS
        id_col, time_col, qty_col = "agg_trade_id", "transact_time", "quantity"
    else:
        raise ValueError(f"unknown Binance data_type {data_type!r}")

    df = _read_archive_csv(zip_path, columns)
    lf = df.lazy().with_columns(
        pl.col(id_col).cast(pl.Utf8).alias("trade_id"),
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col(qty_col).cast(pl.Float64).alias("quantity"),
        pl.col(time_col).cast(pl.Int64).mul(1000).alias("timestamp_utc"),  # ms -> us
        pl.when(pl.col("is_buyer_maker").str.to_lowercase() == "true")
        .then(pl.lit("sell"))
        .otherwise(pl.lit("buy"))
        .alias("side"),
        pl.lit(False).alias("is_liquidation"),
        pl.lit("binance").alias("exchange"),
        pl.lit(market_type).alias("market_type"),
        pl.lit(symbol).alias("symbol"),
    )

    if "quote_qty" in columns:
        lf = lf.with_columns(pl.col("quote_qty").cast(pl.Float64).alias("quote_quantity"))
    else:
        lf = lf.with_columns((pl.col("price") * pl.col("quantity")).alias("quote_quantity"))

    return lf
