"""Task 3.2 (order-book half): spread, mid price, and depth-within-X% metrics.

`add_book_metrics` operates on a flattened L2 snapshot DataFrame shaped
like spec section 3.2 (`orderbook_l2_snapshots`): `bid_price_{i}`/
`bid_size_{i}` and `ask_price_{i}`/`ask_size_{i}` columns for depth
levels 1..N, plus exchange/symbol/timestamp_utc.

NOTE: no exchange this pipeline ingests from publishes that raw
per-level format as a free bulk historical archive - only live trades
are freely available that way. `add_book_metrics` is implemented and
unit-tested against synthetic snapshots so it's ready to plug in if a
raw-level source is ever added (a recorded WebSocket depth stream, or a
paid vendor), but has no real data source today.

`pivot_percentage_depth` is the one partial exception: Binance publishes
a futures `bookDepth` daily archive with *cumulative depth by percentage
bucket* (not raw levels) - see `transform/binance_bookdepth.py`. It can
produce real `bid_depth_usd_Npct`/`ask_depth_usd_Npct` values, but never
`mid_price`/`spread`/`spread_bps` (Binance doesn't publish historical
best-bid/best-ask in bulk form either).
"""

from __future__ import annotations

import polars as pl

DEPTH_LEVELS = 20
DEPTH_PCT_THRESHOLDS = (0.01, 0.02)  # +-1%, +-2%


def add_book_metrics(snapshots: pl.DataFrame, depth_levels: int = DEPTH_LEVELS) -> pl.DataFrame:
    """Add mid_price/spread/spread_bps and cumulative USD depth at +-1%/+-2% to a snapshot frame."""
    df = (
        snapshots.with_columns(
            ((pl.col("bid_price_1") + pl.col("ask_price_1")) / 2).alias("mid_price"),
            (pl.col("ask_price_1") - pl.col("bid_price_1")).alias("spread"),
        )
        .with_columns((pl.col("spread") / pl.col("mid_price") * 10_000).alias("spread_bps"))
    )

    for pct in DEPTH_PCT_THRESHOLDS:
        pct_label = f"{int(pct * 100)}pct"
        bid_terms = [
            pl.when(pl.col(f"bid_price_{i}") >= pl.col("mid_price") * (1 - pct))
            .then(pl.col(f"bid_price_{i}") * pl.col(f"bid_size_{i}"))
            .otherwise(0.0)
            for i in range(1, depth_levels + 1)
            if f"bid_price_{i}" in snapshots.columns
        ]
        ask_terms = [
            pl.when(pl.col(f"ask_price_{i}") <= pl.col("mid_price") * (1 + pct))
            .then(pl.col(f"ask_price_{i}") * pl.col(f"ask_size_{i}"))
            .otherwise(0.0)
            for i in range(1, depth_levels + 1)
            if f"ask_price_{i}" in snapshots.columns
        ]
        df = df.with_columns(
            pl.sum_horizontal(bid_terms).alias(f"bid_depth_usd_{pct_label}"),
            pl.sum_horizontal(ask_terms).alias(f"ask_depth_usd_{pct_label}"),
        )

    return df


def pivot_percentage_depth(book_depth_pct: pl.DataFrame, percentages: tuple[int, ...] = (1, 2)) -> pl.DataFrame:
    """Pivot Binance bookDepth's long format (one row per percentage bucket) into
    one row per (exchange, symbol, timestamp_utc) with `bid_depth_usd_Npct` /
    `ask_depth_usd_Npct` columns - the same column names `add_book_metrics`
    produces, minus mid_price/spread/spread_bps, which this data source
    can't provide (see this module's docstring).
    """
    result = book_depth_pct.select(["exchange", "symbol", "timestamp_utc"]).unique()

    for pct in percentages:
        bid = book_depth_pct.filter(pl.col("percentage") == -pct).select(
            "exchange", "symbol", "timestamp_utc", pl.col("notional").alias(f"bid_depth_usd_{pct}pct")
        )
        ask = book_depth_pct.filter(pl.col("percentage") == pct).select(
            "exchange", "symbol", "timestamp_utc", pl.col("notional").alias(f"ask_depth_usd_{pct}pct")
        )
        result = result.join(bid, on=["exchange", "symbol", "timestamp_utc"], how="left")
        result = result.join(ask, on=["exchange", "symbol", "timestamp_utc"], how="left")

    return result.sort(["exchange", "symbol", "timestamp_utc"])
