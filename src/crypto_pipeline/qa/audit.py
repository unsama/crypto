"""Task 5.1: data quality checks - gap detection and integrity anomalies.

Crypto markets trade continuously, so "gap detection during open market
hours" (spec wording, written with traditional markets in mind) is
implemented here as: any bar timestamp missing from the expected
timeframe-aligned grid between an instrument's first and last observed
bar. Since `resample.resample_trades_to_bars` only emits a bar for a
bucket that actually had >=1 trade, a missing bar is exactly a
zero-volume/gap interval - so this one check covers both of the spec's
"timestamp gap" and "zero-volume" bullets for trade data.
"""

from __future__ import annotations

import polars as pl

from crypto_pipeline.features.resample import TIMEFRAME_EVERY

GAP_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "missing_bar_utc": pl.Datetime("us"),
}


def detect_bar_gaps(bars: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    """One row per missing bar timestamp in each (exchange, symbol) series.

    Only considers the span between each instrument's own first and last
    observed bar - it can't tell you about a gap at the very start/end of
    history, only about holes in the middle.
    """
    if timeframe not in TIMEFRAME_EVERY:
        raise ValueError(f"unsupported timeframe {timeframe!r}")
    if bars.height == 0:
        return pl.DataFrame(schema=GAP_SCHEMA)

    bounds = bars.group_by(["exchange", "symbol"]).agg(
        pl.col("bar_timestamp_utc").min().alias("lo"),
        pl.col("bar_timestamp_utc").max().alias("hi"),
    )

    grids = [
        pl.DataFrame(
            {
                "exchange": exchange,
                "symbol": symbol,
                "missing_bar_utc": pl.datetime_range(lo, hi, interval=timeframe, eager=True, time_unit="us"),
            }
        )
        for exchange, symbol, lo, hi in bounds.iter_rows()
    ]
    full_grid = pl.concat(grids) if grids else pl.DataFrame(schema=GAP_SCHEMA)

    missing = full_grid.join(
        bars.select(["exchange", "symbol", "bar_timestamp_utc"]),
        left_on=["exchange", "symbol", "missing_bar_utc"],
        right_on=["exchange", "symbol", "bar_timestamp_utc"],
        how="anti",
    )
    return missing.sort(["exchange", "symbol", "missing_bar_utc"])


def detect_price_anomalies(trades: pl.DataFrame) -> pl.DataFrame:
    """Flag stored trades violating the pipeline's own invariants.

    Should always be empty if `transform.schema.finalize_trades` ran
    correctly - this is a post-storage sanity check (e.g. after reading
    back from the partitioned lake) rather than a redundant re-filter.
    """
    return trades.filter((pl.col("price") <= 0) | (pl.col("quantity") <= 0) | pl.col("timestamp_utc").is_null())


def detect_negative_spreads(book_metrics: pl.DataFrame) -> pl.DataFrame:
    """Flag order-book snapshots with a crossed/negative spread (bid > ask).

    A crossed book is a data integrity red flag, not a real market
    condition. Requires L2 snapshot data - see `features/orderbook.py`'s
    note on this pipeline not yet ingesting order books.
    """
    return book_metrics.filter(pl.col("spread") < 0)
