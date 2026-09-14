"""Task 5.2: standardized verification-sample exporter (CSV + Parquet)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class SampleExport:
    csv_path: Path
    parquet_path: Path
    row_count: int


def export_sample(
    con: duckdb.DuckDBPyConnection,
    exchange: str,
    symbol: str,
    start: datetime,
    out_dir: Path,
    hours: int = 24,
) -> SampleExport:
    """Export trades for (exchange, symbol) in [start, start + hours) to CSV + Parquet.

    `start` must be timezone-aware (UTC) so the microsecond-epoch bounds
    passed to the query aren't silently shifted by the local timezone.
    """
    if start.tzinfo is None:
        raise ValueError("start must be timezone-aware (UTC)")

    end = start + timedelta(hours=hours)
    start_us = int(start.timestamp() * 1_000_000)
    end_us = int(end.timestamp() * 1_000_000)

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{exchange}_{symbol}_{start:%Y%m%d}_{hours}h"
    csv_path = out_dir / f"{stem}.csv"
    parquet_path = out_dir / f"{stem}.parquet"

    df = con.execute(
        "SELECT * FROM trades WHERE exchange = ? AND symbol = ? AND timestamp_utc >= ? AND timestamp_utc < ? "
        "ORDER BY timestamp_utc",
        [exchange, symbol, start_us, end_us],
    ).pl()

    df.write_csv(csv_path)
    df.write_parquet(parquet_path, compression="zstd", compression_level=3)

    return SampleExport(csv_path=csv_path, parquet_path=parquet_path, row_count=df.height)
