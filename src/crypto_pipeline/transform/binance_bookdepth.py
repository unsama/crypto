"""Binance futures `bookDepth` daily archives -> percentage-bucketed depth schema.

IMPORTANT - unverified against a live file: this machine cannot reach
data.binance.vision (see project memory / CLAUDE.md), so this parser is
written from documented/recalled knowledge of the archive's column
layout, not confirmed against an actual downloaded file. Before relying
on this in production, download one real `bookDepth` daily zip and check
its header row matches `EXPECTED_COLUMNS` below - if Binance's real
columns differ, this will raise a clear KeyError/ColumnNotFoundError
rather than silently mis-parsing.

This is also a fundamentally different shape of data than the
`orderbook_l2_snapshots` schema in spec section 3.2: Binance publishes
cumulative depth/notional in 1%-wide buckets away from the mid price
(percentage = -5..-1, 1..5), not raw per-level bid/ask prices and sizes.
That means it can produce `bid_depth_usd_Npct` / `ask_depth_usd_Npct`
metrics directly, but it *cannot* produce `bid_price_1`, `ask_price_1`,
`mid_price`, or `spread` - Binance doesn't publish historical best-bid/
best-ask in bulk archive form at all. Real per-level L2 history (and
therefore real spread/liquidity-dry-up detection) would need either a
live WebSocket depth-stream recorder running going forward, or a paid
historical data vendor (e.g. Tardis.dev, referenced in the spec's
overview) - it isn't available as a free retroactive download from any
of the three exchanges this pipeline ingests from.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import polars as pl

EXPECTED_COLUMNS = ["timestamp", "percentage", "depth", "notional"]

BOOK_DEPTH_PCT_SCHEMA: dict[str, pl.DataType] = {
    "exchange": pl.Utf8,
    "symbol": pl.Utf8,
    "timestamp_utc": pl.Int64,  # microseconds since epoch
    "percentage": pl.Int64,  # negative = bid side, positive = ask side, e.g. -1 = "within 1% below mid"
    "depth": pl.Float64,  # cumulative base-asset quantity out to this percentage bucket
    "notional": pl.Float64,  # cumulative quote-asset (USD) value out to this percentage bucket
}


def read_binance_book_depth(zip_path: Path, symbol: str) -> pl.LazyFrame:
    """Map a raw Binance futures/um `bookDepth` daily archive onto `BOOK_DEPTH_PCT_SCHEMA`."""
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one .csv member in {zip_path}, found {zf.namelist()}")
        with zf.open(members[0]) as f:
            df = pl.read_csv(f)

    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(
            f"{zip_path}: missing expected bookDepth column(s) {sorted(missing)} - "
            f"found {df.columns}. Binance's real schema may differ from this module's assumption; "
            "see this module's docstring."
        )

    return df.lazy().with_columns(
        pl.col("timestamp").cast(pl.Int64).mul(1000).alias("timestamp_utc"),  # ms -> us
        pl.col("percentage").cast(pl.Int64),
        pl.col("depth").cast(pl.Float64),
        pl.col("notional").cast(pl.Float64),
        pl.lit("binance").alias("exchange"),
        pl.lit(symbol).alias("symbol"),
    ).select(list(BOOK_DEPTH_PCT_SCHEMA.keys()))
