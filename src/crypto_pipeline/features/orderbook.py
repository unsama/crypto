"""Task 3.2 (order-book half): spread, mid price, and depth-within-X% metrics.

Operates on a flattened L2 snapshot DataFrame shaped like spec section 3.2
(`orderbook_l2_snapshots`): `bid_price_{i}`/`bid_size_{i}` and
`ask_price_{i}`/`ask_size_{i}` columns for depth levels 1..N, plus
exchange/symbol/timestamp_utc.

NOTE: this pipeline does not yet ingest L2 order book snapshots - Phase 1
only built trade-tick harvesters (Binance/Bybit/Kraken trades archives),
so there is no real data source feeding this function end-to-end yet.
The transform is implemented and unit-tested against synthetic snapshots
so it's ready to plug in once an order book harvester is added.
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
