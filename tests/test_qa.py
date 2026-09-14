from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from crypto_pipeline.features.resample import resample_trades_to_bars
from crypto_pipeline.qa.audit import detect_bar_gaps, detect_negative_spreads, detect_price_anomalies
from crypto_pipeline.qa.sample_export import export_sample
from crypto_pipeline.qa.summary import generate_summary_report
from crypto_pipeline.storage.partition import write_hive_partitioned
from crypto_pipeline.storage.query import connect
from crypto_pipeline.transform.schema import TRADE_SCHEMA


def _trades(rows: list[dict]) -> pl.DataFrame:
    defaults = {"exchange": "binance", "market_type": "spot", "symbol": "BTC-USDT", "is_liquidation": False}
    full_rows = [{**defaults, **row} for row in rows]
    return pl.DataFrame(full_rows, schema=TRADE_SCHEMA)


def test_detect_bar_gaps_finds_missing_minute():
    base_us = 1_704_067_200_000_000  # 2024-01-01T00:00:00Z
    # Trades at minute 0 and minute 2, nothing in minute 1 -> a gap.
    trades = _trades(
        [
            {"trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": base_us},
            {"trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "buy", "timestamp_utc": base_us + 2 * 60_000_000},
        ]
    )
    bars = resample_trades_to_bars(trades, timeframe="1m")

    gaps = detect_bar_gaps(bars, timeframe="1m")

    assert gaps.height == 1
    row = gaps.row(0, named=True)
    assert row["exchange"] == "binance"
    assert row["symbol"] == "BTC-USDT"
    assert row["missing_bar_utc"] == datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc).replace(tzinfo=None)


def test_detect_bar_gaps_no_gap_when_contiguous():
    base_us = 1_704_067_200_000_000
    trades = _trades(
        [
            {"trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": base_us},
            {"trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "buy", "timestamp_utc": base_us + 60_000_000},
        ]
    )
    bars = resample_trades_to_bars(trades, timeframe="1m")
    gaps = detect_bar_gaps(bars, timeframe="1m")
    assert gaps.height == 0


def test_detect_price_anomalies():
    trades = pl.DataFrame(
        {
            "price": [100.0, -5.0, 0.0, 100.0],
            "quantity": [1.0, 1.0, 1.0, 1.0],
            "timestamp_utc": [1, 2, 3, None],
        },
        schema={"price": pl.Float64, "quantity": pl.Float64, "timestamp_utc": pl.Int64},
    )
    result = detect_price_anomalies(trades)
    assert result.height == 3  # negative price, zero price, null timestamp


def test_detect_negative_spreads():
    book = pl.DataFrame({"spread": [0.1, -0.2, 0.0]})
    result = detect_negative_spreads(book)
    assert result.height == 1


def test_generate_summary_report(tmp_path: Path):
    base_us = 1_704_067_200_000_000
    trades = _trades(
        [
            {"trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": base_us},
            {"trade_id": "2", "price": 105.0, "quantity": 1.0, "quote_quantity": 105.0, "side": "sell", "timestamp_utc": base_us + 60_000_000},
        ]
    )
    write_hive_partitioned(trades, tmp_path, "trades", "timestamp_utc")

    con = connect(tmp_path)
    report = generate_summary_report(con)

    assert "# Data Quality Summary" in report
    assert "binance" in report
    assert "BTC-USDT" in report
    assert "2" in report  # row count


def test_generate_summary_report_no_trades(tmp_path: Path):
    con = connect(tmp_path)
    report = generate_summary_report(con)
    assert "No `trades` dataset found" in report


def test_export_sample(tmp_path: Path):
    day1 = datetime(2024, 1, 1, tzinfo=timezone.utc)
    day1_us = int(day1.timestamp() * 1_000_000)
    day2_us = day1_us + 25 * 3_600_000_000  # into the next day, outside a 24h window from day1

    trades = _trades(
        [
            {"trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": day1_us},
            {"trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "buy", "timestamp_utc": day1_us + 3_600_000_000},
            {"trade_id": "3", "price": 999.0, "quantity": 1.0, "quote_quantity": 999.0, "side": "buy", "timestamp_utc": day2_us},
        ]
    )
    write_hive_partitioned(trades, tmp_path, "trades", "timestamp_utc")
    con = connect(tmp_path)

    out_dir = tmp_path / "samples"
    result = export_sample(con, exchange="binance", symbol="BTC-USDT", start=day1, out_dir=out_dir, hours=24)

    assert result.row_count == 2
    assert result.csv_path.exists()
    assert result.parquet_path.exists()
    exported = pl.read_parquet(result.parquet_path)
    assert set(exported["trade_id"]) == {"1", "2"}
