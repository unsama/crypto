"""Unified `trades_tick` schema (spec section 3.1) and the Task 2.2 cleaning pass."""

from __future__ import annotations

import polars as pl

TRADE_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,
    "market_type": pl.Utf8,
    "symbol": pl.Utf8,
    "timestamp_utc": pl.Int64,  # microseconds since epoch
    "trade_id": pl.Utf8,
    "price": pl.Float64,
    "quantity": pl.Float64,
    "quote_quantity": pl.Float64,
    "side": pl.Utf8,
    "is_liquidation": pl.Boolean,
}

TRADE_COLUMNS = list(TRADE_SCHEMA.keys())


def finalize_trades(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Task 2.2: cast to the unified schema, drop invalid rows, sort, and dedupe.

    Invalid rows (non-positive price/quantity, missing timestamp or id) are
    dropped rather than raising, per the spec's zero-corruption acceptance
    criterion (section 6) - a single malformed row in a multi-million-row
    archive should not fail the whole batch.

    Sorting before dedup makes the result deterministic regardless of the
    raw file's row order (section 6: deterministic re-runs).
    """
    casts = [
        pl.col(name).cast(dtype).alias(name) for name, dtype in TRADE_SCHEMA.items() if name != "is_liquidation"
    ]
    casts.append(pl.col("is_liquidation").cast(pl.Boolean).fill_null(False).alias("is_liquidation"))

    return (
        lf.select(casts)
        .filter(
            pl.col("timestamp_utc").is_not_null()
            & pl.col("trade_id").is_not_null()
            & (pl.col("price") > 0)
            & (pl.col("quantity") > 0)
        )
        .sort(["exchange", "symbol", "timestamp_utc", "trade_id"])
        .unique(subset=["exchange", "symbol", "trade_id"], keep="first", maintain_order=True)
    )
