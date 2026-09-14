"""Task 3.2 (price-derived half): rolling realized volatility from bar closes.

sigma = sqrt(sum(r_t^2)) over a trailing window of bar-to-bar log returns
(spec section 5, Phase 3 Task 3.2). Order-book-derived metrics (spread,
depth) live in `orderbook.py`.
"""

from __future__ import annotations

import polars as pl


def add_realized_volatility(bars: pl.DataFrame, window_bars: int, column_name: str) -> pl.DataFrame:
    """Add a trailing realized-volatility column computed over `window_bars` closes.

    Operates independently per (exchange, symbol); `bars` must already be
    sorted by bar_timestamp_utc within each symbol, as
    `resample.resample_trades_to_bars` returns.
    """
    if window_bars < 1:
        raise ValueError(f"window_bars must be >= 1, got {window_bars}")

    grp = ["exchange", "symbol"]
    log_return = pl.col("close").log() - pl.col("close").log().shift(1).over(grp)

    return bars.with_columns(log_return.alias("_log_return")).with_columns(
        (pl.col("_log_return") ** 2)
        .rolling_sum(window_size=window_bars, min_samples=1)
        .over(grp)
        .sqrt()
        .alias(column_name)
    ).drop("_log_return")
