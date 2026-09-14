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


def extract_csv_member(zip_path: Path, dest_dir: Path) -> Path:
    """Extract the single CSV member of a Binance archive zip to `dest_dir`, return its path.

    A sequential disk-to-disk decompression (zipfile streams in bounded
    chunks internally), so this holds only a small buffer in memory
    regardless of file size - unlike reading the member into memory
    first, which would need to hold the entire decompressed CSV at once.
    """
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one .csv member in {zip_path}, found {members}")
        extracted = zf.extract(members[0], path=dest_dir)
    return Path(extracted)


def _read_archive_csv(csv_path: Path, columns: list[str]) -> pl.LazyFrame:
    """Lazily scan an already-extracted Binance trades/aggTrades CSV with native dtypes.

    Binance archives are inconsistent about whether they include a header
    row, so a cheap first-line peek decides whether to skip a header
    (letting polars infer real dtypes from the data) versus scanning
    headerless with explicit column names. `scan_csv` (not `read_csv`)
    keeps this lazy so the caller's full read -> transform -> filter ->
    sort -> write chain can run through polars' streaming engine without
    ever materializing the whole file as one in-memory DataFrame - a
    real multi-million-row monthly archive OOM'd under an earlier eager,
    all-Utf8-typed version of this function.
    """
    with csv_path.open("r", encoding="utf-8", errors="replace") as f:
        first_line = f.readline()
    first_field = first_line.split(",", 1)[0].strip()
    has_header = not _looks_numeric(first_field)

    if has_header:
        lf = pl.scan_csv(csv_path)
        lf = lf.rename(dict(zip(lf.collect_schema().names(), columns[: len(lf.collect_schema().names())])))
    else:
        lf = pl.scan_csv(csv_path, has_header=False, new_columns=columns)

    return lf


def read_binance_trades(csv_path: Path, market: str, symbol: str, data_type: str = "trades") -> pl.LazyFrame:
    """Map an already-extracted Binance trades/aggTrades CSV onto the unified trades_tick schema."""
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

    lf = _read_archive_csv(csv_path, columns).with_columns(
        pl.col(id_col).cast(pl.Utf8).alias("trade_id"),
        pl.col("price").cast(pl.Float64).alias("price"),
        pl.col(qty_col).cast(pl.Float64).alias("quantity"),
        pl.col(time_col).cast(pl.Int64).mul(1000).alias("timestamp_utc"),  # ms -> us
        pl.when(pl.col("is_buyer_maker").cast(pl.Utf8).str.to_lowercase() == "true")
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
