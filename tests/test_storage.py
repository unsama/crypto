from pathlib import Path

import polars as pl
import pytest

from crypto_pipeline.storage.partition import write_hive_partitioned
from crypto_pipeline.storage.query import connect
from crypto_pipeline.transform.schema import TRADE_SCHEMA


def _trades(rows: list[dict]) -> pl.DataFrame:
    defaults = {"exchange": "binance", "market_type": "spot", "is_liquidation": False}
    full_rows = [{**defaults, **row} for row in rows]
    return pl.DataFrame(full_rows, schema=TRADE_SCHEMA)


def test_write_hive_partitioned_creates_expected_path(tmp_path: Path):
    jan_us = 1_704_067_200_000_000  # 2024-01-01T00:00:00Z
    df = _trades(
        [
            {"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us},
        ]
    )

    written = write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc")

    expected = tmp_path / "trades" / "exchange=binance" / "symbol=BTC-USDT" / "year=2024" / "month=01" / "data.parquet"
    assert written == [expected]
    assert expected.exists()
    assert pl.read_parquet(expected).height == 1


def test_write_hive_partitioned_splits_by_symbol_and_month(tmp_path: Path):
    jan_us = 1_704_067_200_000_000
    feb_us = 1_706_745_600_000_000  # 2024-02-01T00:00:00Z
    df = _trades(
        [
            {"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us},
            {"symbol": "BTC-USDT", "trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "buy", "timestamp_utc": feb_us},
            {"symbol": "ETH-USDT", "trade_id": "3", "price": 50.0, "quantity": 1.0, "quote_quantity": 50.0, "side": "buy", "timestamp_utc": jan_us},
        ]
    )

    written = write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc")

    assert len(written) == 3


def test_write_hive_partitioned_overwrite_without_merge_key_is_deterministic(tmp_path: Path):
    jan_us = 1_704_067_200_000_000
    df = _trades(
        [
            {"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us},
        ]
    )

    write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc")
    write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc")  # re-run same batch

    dest = tmp_path / "trades" / "exchange=binance" / "symbol=BTC-USDT" / "year=2024" / "month=01" / "data.parquet"
    assert pl.read_parquet(dest).height == 1  # not duplicated


def test_write_hive_partitioned_merge_key_dedupes_across_runs(tmp_path: Path):
    jan_us = 1_704_067_200_000_000
    batch1 = _trades(
        [{"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us}]
    )
    batch2 = _trades(
        [
            {"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us},
            {"symbol": "BTC-USDT", "trade_id": "2", "price": 102.0, "quantity": 1.0, "quote_quantity": 102.0, "side": "sell", "timestamp_utc": jan_us + 1_000_000},
        ]
    )

    merge_key = ["exchange", "symbol", "trade_id"]
    write_hive_partitioned(batch1, tmp_path, "trades", "timestamp_utc", merge_key=merge_key)
    write_hive_partitioned(batch2, tmp_path, "trades", "timestamp_utc", merge_key=merge_key)

    dest = tmp_path / "trades" / "exchange=binance" / "symbol=BTC-USDT" / "year=2024" / "month=01" / "data.parquet"
    result = pl.read_parquet(dest)
    assert result.height == 2
    assert set(result["trade_id"]) == {"1", "2"}


def test_write_hive_partitioned_empty_frame_writes_nothing(tmp_path: Path):
    df = pl.DataFrame(schema=TRADE_SCHEMA)
    written = write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc")
    assert written == []


def test_write_hive_partitioned_part_name_writes_separate_files(tmp_path: Path):
    jan_us = 1_704_067_200_000_000
    chunk1 = _trades(
        [{"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us}]
    )
    chunk2 = _trades(
        [{"symbol": "BTC-USDT", "trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "sell", "timestamp_utc": jan_us + 1_000_000}]
    )

    written1 = write_hive_partitioned(chunk1, tmp_path, "trades", "timestamp_utc", part_name="0000")
    written2 = write_hive_partitioned(chunk2, tmp_path, "trades", "timestamp_utc", part_name="0001")

    partition_dir = tmp_path / "trades" / "exchange=binance" / "symbol=BTC-USDT" / "year=2024" / "month=01"
    assert written1 == [partition_dir / "part-0000.parquet"]
    assert written2 == [partition_dir / "part-0001.parquet"]
    assert (partition_dir / "part-0000.parquet").exists()
    assert (partition_dir / "part-0001.parquet").exists()
    # each part file holds only its own chunk, no read-back/merge of the other
    assert pl.read_parquet(partition_dir / "part-0000.parquet").height == 1
    assert pl.read_parquet(partition_dir / "part-0001.parquet").height == 1


def test_write_hive_partitioned_part_name_and_merge_key_conflict(tmp_path: Path):
    df = _trades(
        [{"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": 1_704_067_200_000_000}]
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc", merge_key=["trade_id"], part_name="0000")


def test_connect_and_query_lake(tmp_path: Path):
    jan_us = 1_704_067_200_000_000
    df = _trades(
        [
            {"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 2.0, "quote_quantity": 200.0, "side": "buy", "timestamp_utc": jan_us},
            {"symbol": "BTC-USDT", "trade_id": "2", "price": 101.0, "quantity": 3.0, "quote_quantity": 303.0, "side": "sell", "timestamp_utc": jan_us + 1_000_000},
        ]
    )
    write_hive_partitioned(df, tmp_path, "trades", "timestamp_utc")

    con = connect(tmp_path)
    row = con.sql("SELECT COUNT(*) AS n, SUM(quantity) AS total_qty FROM trades").fetchone()
    assert row[0] == 2
    assert row[1] == 5.0

    # DuckDB infers hive partition value types itself (e.g. month "01" -> int), so cast explicitly.
    partition_row = con.sql(
        "SELECT DISTINCT exchange, symbol, CAST(year AS INTEGER), CAST(month AS INTEGER) FROM trades"
    ).fetchone()
    assert partition_row == ("binance", "BTC-USDT", 2024, 1)


def test_connect_queries_across_multiple_part_files(tmp_path: Path):
    jan_us = 1_704_067_200_000_000
    chunk1 = _trades(
        [{"symbol": "BTC-USDT", "trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": jan_us}]
    )
    chunk2 = _trades(
        [{"symbol": "BTC-USDT", "trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "sell", "timestamp_utc": jan_us + 1_000_000}]
    )
    write_hive_partitioned(chunk1, tmp_path, "trades", "timestamp_utc", part_name="0000")
    write_hive_partitioned(chunk2, tmp_path, "trades", "timestamp_utc", part_name="0001")

    con = connect(tmp_path)
    row = con.sql("SELECT COUNT(*) AS n FROM trades").fetchone()
    assert row[0] == 2
