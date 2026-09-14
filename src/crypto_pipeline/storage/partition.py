"""Task 4.1: hive-partitioned Parquet storage.

Writes a DataFrame out as
`<root>/<dataset>/exchange=<exchange>/symbol=<symbol>/year=<YYYY>/month=<MM>/data.parquet`
(spec section 5's `output_lake/...` layout, under this repo's existing
`data/gold/` medallion root rather than a separate top-level name) - one
partition file per (exchange, symbol, year, month) combination present
in the input.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

# Parquet row groups are sized by row count, not bytes, so this is an
# approximation of the spec's 128-512MB target for the trades_tick /
# ohlcv_bars column widths - tune based on observed file sizes.
ROW_GROUP_SIZE_ROWS = 1_000_000


def _with_year_month(df: pl.DataFrame, time_column: str) -> pl.DataFrame:
    if df.schema[time_column] == pl.Int64:
        dt_expr = pl.from_epoch(pl.col(time_column), time_unit="us")
    else:
        dt_expr = pl.col(time_column)
    return df.with_columns(dt_expr.dt.year().alias("_year"), dt_expr.dt.month().alias("_month"))


def write_hive_partitioned(
    df: pl.DataFrame,
    root: Path,
    dataset: str,
    time_column: str,
    merge_key: list[str] | None = None,
) -> list[Path]:
    """Write `df` into hive partitions under `root/dataset/exchange=.../symbol=.../year=.../month=.../data.parquet`.

    If `merge_key` is given and a partition file already exists, new rows
    are merged with the existing file (concatenated, deduped on
    `merge_key` keeping the new rows, re-sorted by `time_column`) instead
    of overwriting it outright - so re-running with an overlapping batch
    stays idempotent rather than duplicating rows. Without `merge_key`,
    each touched partition file is fully overwritten from `df` alone,
    which is what gives identical re-runs of the same batch bit-identical
    output (spec section 6's determinism criterion).
    """
    if df.height == 0:
        return []

    df = _with_year_month(df, time_column)
    written: list[Path] = []

    for (exchange, symbol, year, month), group in df.group_by(["exchange", "symbol", "_year", "_month"]):
        partition_dir = (
            root / dataset / f"exchange={exchange}" / f"symbol={symbol}" / f"year={year}" / f"month={month:02d}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        dest_path = partition_dir / "data.parquet"

        out = group.drop(["_year", "_month"])
        if merge_key and dest_path.exists():
            existing = pl.read_parquet(dest_path)
            out = (
                pl.concat([existing, out], how="vertical_relaxed")
                .unique(subset=merge_key, keep="last", maintain_order=True)
                .sort(time_column)
            )
        else:
            out = out.sort(time_column)

        out.write_parquet(
            dest_path,
            compression="zstd",
            compression_level=3,
            row_group_size=ROW_GROUP_SIZE_ROWS,
        )
        logger.info("wrote %d row(s) -> %s", out.height, dest_path)
        written.append(dest_path)

    return written
