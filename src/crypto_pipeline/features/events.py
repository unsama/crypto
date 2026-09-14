"""Task 3.3: configurable market-event tagging.

`tag_bar_events` operates on OHLCV bars (as produced by
`resample.resample_trades_to_bars`) for flash-move and volume-surge
rules. `tag_liquidity_dry_ups` operates on order-book metrics (from
`orderbook.add_book_metrics`) - see that module's note on L2 data not
yet being ingested.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class EventThresholds:
    flash_move_pct: float = 3.0
    """Flag a bar if |price change| over `flash_move_window_bars` exceeds this, in percent."""
    flash_move_window_bars: int = 1

    spread_blowout_bps: float = 50.0
    """Flag a book snapshot whose spread_bps exceeds this as a liquidity dry-up."""

    volume_surge_zscore: float = 3.0
    """Flag a bar whose rolling volume Z-score exceeds this as a volume surge."""
    volume_surge_window_bars: int = 20


def tag_flash_moves(bars: pl.DataFrame, thresholds: EventThresholds = EventThresholds()) -> pl.DataFrame:
    """Flag flash crashes/spikes: |close / close[t-w] - 1| > flash_move_pct, per (exchange, symbol)."""
    grp = ["exchange", "symbol"]
    w = thresholds.flash_move_window_bars
    return bars.with_columns(
        ((pl.col("close") / pl.col("close").shift(w).over(grp) - 1) * 100).alias("price_change_pct")
    ).with_columns((pl.col("price_change_pct").abs() > thresholds.flash_move_pct).fill_null(False).alias("is_flash_move"))


def tag_volume_surges(bars: pl.DataFrame, thresholds: EventThresholds = EventThresholds()) -> pl.DataFrame:
    """Flag volume surge anomalies: current volume_base vs a Z-score baseline of the *prior* bars.

    The baseline window excludes the current bar - including it would let
    the surge itself inflate the mean/std it's being compared against,
    diluting exactly the anomaly this is meant to catch.
    """
    grp = ["exchange", "symbol"]
    w = thresholds.volume_surge_window_bars
    bars = bars.with_columns(pl.col("volume_base").shift(1).over(grp).alias("_prior_volume"))
    rolling_mean = pl.col("_prior_volume").rolling_mean(window_size=w, min_samples=2).over(grp)
    rolling_std = pl.col("_prior_volume").rolling_std(window_size=w, min_samples=2).over(grp)
    return (
        bars.with_columns(((pl.col("volume_base") - rolling_mean) / rolling_std).alias("volume_zscore"))
        .with_columns((pl.col("volume_zscore") > thresholds.volume_surge_zscore).fill_null(False).alias("is_volume_surge"))
        .drop("_prior_volume")
    )


def tag_bar_events(bars: pl.DataFrame, thresholds: EventThresholds = EventThresholds()) -> pl.DataFrame:
    """Apply both bar-level rules (flash moves, volume surges) in one pass."""
    return tag_volume_surges(tag_flash_moves(bars, thresholds), thresholds)


def tag_liquidity_dry_ups(book_metrics: pl.DataFrame, thresholds: EventThresholds = EventThresholds()) -> pl.DataFrame:
    """Flag liquidity dry-ups: spread_bps > spread_blowout_bps (requires order-book snapshot data)."""
    return book_metrics.with_columns(
        (pl.col("spread_bps") > thresholds.spread_blowout_bps).alias("is_liquidity_dry_up")
    )
