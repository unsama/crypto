import zipfile
from pathlib import Path

import polars as pl
import pytest

from crypto_pipeline.features.orderbook import pivot_percentage_depth
from crypto_pipeline.transform.binance_bookdepth import read_binance_book_depth


def _make_bookdepth_zip(tmp_path: Path, rows: list[str], name: str = "BTCUSDT-bookDepth-2024-01-01") -> Path:
    zip_path = tmp_path / f"{name}.zip"
    lines = ["timestamp,percentage,depth,notional"] + rows
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(f"{name}.csv", "\n".join(lines) + "\n")
    return zip_path


def test_read_binance_book_depth_maps_schema(tmp_path: Path):
    rows = [
        "1704067200000,-1,1.5,75000.0",
        "1704067200000,1,2.0,100100.0",
    ]
    zip_path = _make_bookdepth_zip(tmp_path, rows)

    df = read_binance_book_depth(zip_path, symbol="BTCUSDT").collect()

    assert df.height == 2
    assert set(df["exchange"]) == {"binance"}
    assert set(df["symbol"]) == {"BTCUSDT"}
    row = df.filter(pl.col("percentage") == -1).row(0, named=True)
    assert row["timestamp_utc"] == 1704067200000 * 1000
    assert row["depth"] == 1.5
    assert row["notional"] == 75000.0


def test_read_binance_book_depth_raises_on_unexpected_schema(tmp_path: Path):
    zip_path = _make_bookdepth_zip(tmp_path, ["1704067200000,-1,1.5,75000.0"])
    # Overwrite with a file that has a totally different header, simulating
    # Binance's real schema differing from this module's assumption.
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("BTCUSDT-bookDepth-2024-01-01.csv", "foo,bar\n1,2\n")

    with pytest.raises(ValueError, match="missing expected bookDepth column"):
        read_binance_book_depth(zip_path, symbol="BTCUSDT").collect()


def test_pivot_percentage_depth():
    df = pl.DataFrame(
        {
            "exchange": ["binance"] * 4,
            "symbol": ["BTC-USDT"] * 4,
            "timestamp_utc": [1000, 1000, 1000, 1000],
            "percentage": [-1, -2, 1, 2],
            "depth": [1.0, 2.0, 1.5, 2.5],
            "notional": [50000.0, 95000.0, 75500.0, 124000.0],
        }
    )

    result = pivot_percentage_depth(df, percentages=(1, 2))
    row = result.row(0, named=True)

    assert row["bid_depth_usd_1pct"] == 50000.0
    assert row["bid_depth_usd_2pct"] == 95000.0
    assert row["ask_depth_usd_1pct"] == 75500.0
    assert row["ask_depth_usd_2pct"] == 124000.0
