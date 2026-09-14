"""Task 3.1: resample unified trades_tick rows into OHLCV bars (spec section 3.3 schema)."""

from __future__ import annotations

import polars as pl

BAR_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "timeframe": pl.Utf8,
    "bar_timestamp_utc": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume_base": pl.Float64,
    "volume_quote": pl.Float64,
    "trade_count": pl.Int64,
    "buy_volume_base": pl.Float64,
    "sell_volume_base": pl.Float64,
    "volume_delta": pl.Float64,
    "vwap": pl.Float64,
}

BAR_COLUMNS = list(BAR_SCHEMA.keys())

# Polars duration-string form of each timeframe (also reused by qa/audit.py
# to build the expected full time grid for gap detection).
TIMEFRAME_EVERY = {"1s": "1s", "1m": "1m", "5m": "5m"}
TIMEFRAME_SECONDS = {"1s": 1, "1m": 60, "5m": 300}


def bars_per_window(bar_timeframe: str, window: str) -> int | None:
    """How many `bar_timeframe` bars fit in `window` (e.g. "1s","1m" -> 60), or None if it doesn't divide evenly."""
    bar_secs = TIMEFRAME_SECONDS[bar_timeframe]
    window_secs = TIMEFRAME_SECONDS[window]
    if window_secs < bar_secs or window_secs % bar_secs != 0:
        return None
    return window_secs // bar_secs


def resample_trades_to_bars(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    """Resample unified trades_tick rows (any mix of exchanges/symbols) into OHLCV bars.

    Groups by (exchange, symbol) so multiple instruments can be passed in
    the same DataFrame. VWAP and buy/sell volume split use the taker
    `side` and `quantity`/`quote_quantity` columns from the trades_tick
    schema (spec section 3.1).
    """
    if timeframe not in TIMEFRAME_EVERY:
        raise ValueError(f"unsupported timeframe {timeframe!r}")

    if df.height == 0:
        return pl.DataFrame(schema=BAR_SCHEMA)

    every = TIMEFRAME_EVERY[timeframe]
    bars = (
        df.lazy()
        .with_columns(pl.from_epoch("timestamp_utc", time_unit="us").alias("_ts"))
        .sort("_ts")
        .group_by_dynamic("_ts", every=every, closed="left", label="left", group_by=["exchange", "symbol"])
        .agg(
            open=pl.col("price").first(),
            high=pl.col("price").max(),
            low=pl.col("price").min(),
            close=pl.col("price").last(),
            volume_base=pl.col("quantity").sum(),
            volume_quote=pl.col("quote_quantity").sum(),
            trade_count=pl.len(),
            buy_volume_base=pl.col("quantity").filter(pl.col("side") == "buy").sum().fill_null(0.0),
            sell_volume_base=pl.col("quantity").filter(pl.col("side") == "sell").sum().fill_null(0.0),
            vwap=(pl.col("price") * pl.col("quantity")).sum() / pl.col("quantity").sum(),
        )
        .with_columns(
            (pl.col("buy_volume_base") - pl.col("sell_volume_base")).alias("volume_delta"),
            pl.lit(timeframe).alias("timeframe"),
        )
        .rename({"_ts": "bar_timestamp_utc"})
        .select(BAR_COLUMNS)
        .sort(["exchange", "symbol", "bar_timestamp_utc"])
    )
    return bars.collect()
