import gzip
import json
import zipfile
from pathlib import Path

from crypto_pipeline.transform.pipeline import (
    normalize_binance_file,
    normalize_bybit_file,
    normalize_kraken_pair,
)
from crypto_pipeline.transform.schema import TRADE_COLUMNS
from crypto_pipeline.transform.symbols import normalize_symbol


def _make_binance_zip(tmp_path: Path, rows: list[str], header: str | None, name: str = "BTCUSDT-trades-2024-01") -> Path:
    zip_path = tmp_path / f"{name}.zip"
    lines = ([header] if header else []) + rows
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(f"{name}.csv", "\n".join(lines) + "\n")
    return zip_path


def test_normalize_binance_file_no_header(tmp_path: Path):
    rows = [
        "1,50000.5,0.1,5000.05,1704067200000,True,True",  # sell (buyer is maker)
        "2,50001.0,0.2,10000.20,1704067201000,False,True",  # buy
    ]
    zip_path = _make_binance_zip(tmp_path, rows, header=None)

    df = normalize_binance_file(zip_path, market="spot", symbol="BTCUSDT", data_type="trades")

    assert df.columns == TRADE_COLUMNS
    assert df.height == 2
    assert set(df["symbol"]) == {"BTC-USDT"}
    assert set(df["exchange"]) == {"binance"}
    assert set(df["market_type"]) == {"spot"}
    row1 = df.filter(df["trade_id"] == "1").row(0, named=True)
    assert row1["side"] == "sell"
    assert row1["timestamp_utc"] == 1704067200000 * 1000
    row2 = df.filter(df["trade_id"] == "2").row(0, named=True)
    assert row2["side"] == "buy"
    assert row2["quote_quantity"] == 10000.20


def test_normalize_binance_file_with_header_row(tmp_path: Path):
    header = "id,price,qty,quote_qty,time,is_buyer_maker,is_best_match"
    rows = ["1,50000.5,0.1,5000.05,1704067200000,True,True"]
    zip_path = _make_binance_zip(tmp_path, rows, header=header)

    df = normalize_binance_file(zip_path, market="spot", symbol="ETHUSDT", data_type="trades")

    assert df.height == 1
    assert df["symbol"][0] == "ETH-USDT"


def test_normalize_binance_dedupes_and_sorts(tmp_path: Path):
    rows = [
        "2,50001.0,0.2,10000.20,1704067201000,False,True",
        "1,50000.5,0.1,5000.05,1704067200000,True,True",
        "1,50000.5,0.1,5000.05,1704067200000,True,True",  # exact duplicate row
    ]
    zip_path = _make_binance_zip(tmp_path, rows, header=None)

    df = normalize_binance_file(zip_path, market="spot", symbol="BTCUSDT", data_type="trades")

    assert df.height == 2  # duplicate trade_id collapsed
    assert df["timestamp_utc"].to_list() == sorted(df["timestamp_utc"].to_list())


def test_normalize_binance_drops_invalid_rows(tmp_path: Path):
    rows = [
        "1,0,0.1,0,1704067200000,True,True",  # zero price -> dropped
        "2,50001.0,0.2,10000.20,1704067201000,False,True",  # valid
    ]
    zip_path = _make_binance_zip(tmp_path, rows, header=None)

    df = normalize_binance_file(zip_path, market="spot", symbol="BTCUSDT", data_type="trades")

    assert df.height == 1
    assert df["trade_id"][0] == "2"


def test_normalize_bybit_file(tmp_path: Path):
    header = "timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional"
    rows = [
        "1704067200.123456,BTCUSDT,Buy,0.5,50000.0,PlusTick,abc-123,0,0.5,25000.0",
        "1704067201.654321,BTCUSDT,Sell,0.25,50010.0,MinusTick,def-456,0,0.25,12502.5",
    ]
    csv_gz_path = tmp_path / "BTCUSDT2024-01-01.csv.gz"
    with gzip.open(csv_gz_path, "wt") as f:
        f.write("\n".join([header] + rows) + "\n")

    df = normalize_bybit_file(csv_gz_path, symbol="BTCUSDT", market="linear")

    assert df.columns == TRADE_COLUMNS
    assert df.height == 2
    assert set(df["exchange"]) == {"bybit"}
    assert set(df["market_type"]) == {"linear_perpetual"}
    buy_row = df.filter(df["trade_id"] == "abc-123").row(0, named=True)
    assert buy_row["side"] == "buy"
    assert buy_row["quantity"] == 0.5
    assert buy_row["quote_quantity"] == 25000.0


def test_normalize_kraken_pair_mixed_id_formats(tmp_path: Path):
    pair_dir = tmp_path / "XBTUSD"
    pair_dir.mkdir()

    # Page with native trade_id (newer API shape).
    page1 = {
        "error": [],
        "result": {
            "XXBTZUSD": [[50000.1, "0.5", 1704067200.111, "b", "m", "", 999]],
            "last": "1704067200111000000",
        },
    }
    (pair_dir / "page_1.json").write_text(json.dumps(page1))

    # Page without a native trade_id (older API shape).
    page2 = {
        "error": [],
        "result": {
            "XXBTZUSD": [[50010.0, "0.25", 1704067260.222, "s", "l", ""]],
            "last": "1704067260222000000",
        },
    }
    (pair_dir / "page_2.json").write_text(json.dumps(page2))

    df = normalize_kraken_pair(pair_dir, pair="XXBTZUSD")

    assert df.height == 2
    assert set(df["symbol"]) == {"BTC-USD"}
    assert set(df["exchange"]) == {"kraken"}
    native_id_row = df.filter(df["trade_id"] == "999").row(0, named=True)
    assert native_id_row["side"] == "buy"
    assert native_id_row["quantity"] == 0.5
    synthetic_id_row = df.filter(df["side"] == "sell").row(0, named=True)
    assert synthetic_id_row["trade_id"] == "1704067260.222-0"


def test_normalize_symbol_binance_bybit():
    assert normalize_symbol("BTCUSDT", "binance") == "BTC-USDT"
    assert normalize_symbol("ETHUSD", "bybit") == "ETH-USD"


def test_normalize_symbol_kraken_legacy_aliases():
    assert normalize_symbol("XXBTZUSD", "kraken") == "BTC-USD"
    assert normalize_symbol("XBTUSD", "kraken") == "BTC-USD"
    assert normalize_symbol("XETHZUSD", "kraken") == "ETH-USD"
