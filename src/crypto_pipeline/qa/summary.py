"""Task 5.1: automated data-quality summary report (markdown).

Reads directly from a DuckDB connection opened by `storage.query.connect`
(one view per dataset dir), so it works against any partitioned lake
without extra loading.
"""

from __future__ import annotations

import duckdb
import polars as pl

from crypto_pipeline.qa.audit import detect_bar_gaps


def generate_summary_report(con: duckdb.DuckDBPyConnection) -> str:
    """Build a markdown report: row counts, date ranges, min/max prices per
    (exchange, symbol), plus bar-gap counts per timeframe if a `bars`
    dataset is present.
    """
    tables = {row[0] for row in con.sql("SHOW TABLES").fetchall()}
    lines = ["# Data Quality Summary", ""]

    if "trades" not in tables:
        lines.append("_No `trades` dataset found under the lake root._")
        return "\n".join(lines)

    stats = con.sql(
        """
        SELECT
            exchange,
            symbol,
            COUNT(*) AS row_count,
            make_timestamp(MIN(timestamp_utc)) AS first_trade_utc,
            make_timestamp(MAX(timestamp_utc)) AS last_trade_utc,
            MIN(price) AS min_price,
            MAX(price) AS max_price,
            SUM(CASE WHEN price <= 0 OR quantity <= 0 THEN 1 ELSE 0 END) AS invalid_rows
        FROM trades
        GROUP BY exchange, symbol
        ORDER BY exchange, symbol
        """
    ).pl()

    lines.append("## Trades")
    lines.append("")
    lines.append("| Exchange | Symbol | Rows | First Trade (UTC) | Last Trade (UTC) | Min Price | Max Price | Invalid Rows |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for row in stats.iter_rows(named=True):
        lines.append(
            f"| {row['exchange']} | {row['symbol']} | {row['row_count']:,} | {row['first_trade_utc']} | "
            f"{row['last_trade_utc']} | {row['min_price']:g} | {row['max_price']:g} | {row['invalid_rows']} |"
        )

    if "bars" in tables:
        bars_df: pl.DataFrame = con.sql("SELECT * FROM bars").pl()
        lines.append("")
        lines.append("## Bar Gaps")
        lines.append("")
        if bars_df.height == 0:
            lines.append("_No bars found._")
        else:
            lines.append("| Exchange | Symbol | Timeframe | Missing Bars |")
            lines.append("|---|---|---|---|")
            for (exchange, symbol, timeframe), group in bars_df.group_by(["exchange", "symbol", "timeframe"]):
                gaps = detect_bar_gaps(group, timeframe)
                lines.append(f"| {exchange} | {symbol} | {timeframe} | {gaps.height} |")

    return "\n".join(lines)
