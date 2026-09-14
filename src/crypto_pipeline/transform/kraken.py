"""Kraken raw trade pages (REST pagination dumps) -> unified trades_tick schema (Task 2.1).

Reads the `*.json` pages written by `KrakenTradesFetcher` (one file per
`since` cursor, containing the raw API response). Kraken's public Trades
endpoint only covers spot markets.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

MARKET_TYPE = "spot"


def read_kraken_trades(pair_dir: Path, symbol: str) -> pl.LazyFrame:
    """Map every raw trade page under `pair_dir` onto the unified trades_tick schema."""
    rows: list[dict] = []
    for page_path in sorted(pair_dir.glob("*.json")):
        payload = json.loads(page_path.read_text())
        result = payload["result"]
        trade_rows = next(v for k, v in result.items() if k != "last")
        for i, row in enumerate(trade_rows):
            price, volume, ts, side = row[0], row[1], row[2], row[3]
            # Older Kraken API responses omit a native trade id; synthesize
            # one from (timestamp, position-in-page) so it's still unique
            # and stable across re-runs of the same cached page.
            trade_id = str(row[6]) if len(row) > 6 else f"{ts}-{i}"
            rows.append(
                {
                    "trade_id": trade_id,
                    "price": float(price),
                    "quantity": float(volume),
                    "timestamp_s": float(ts),
                    "side": "buy" if side == "b" else "sell",
                }
            )

    df = pl.DataFrame(
        rows,
        schema={
            "trade_id": pl.Utf8,
            "price": pl.Float64,
            "quantity": pl.Float64,
            "timestamp_s": pl.Float64,
            "side": pl.Utf8,
        },
    )
    lf = df.lazy().with_columns(
        (pl.col("timestamp_s") * 1_000_000).round(0).cast(pl.Int64).alias("timestamp_utc"),
        (pl.col("price") * pl.col("quantity")).alias("quote_quantity"),
        pl.lit(False).alias("is_liquidation"),
        pl.lit("kraken").alias("exchange"),
        pl.lit(MARKET_TYPE).alias("market_type"),
        pl.lit(symbol).alias("symbol"),
    )
    return lf
