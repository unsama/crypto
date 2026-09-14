import math

import polars as pl

from crypto_pipeline.features.events import (
    EventThresholds,
    tag_flash_moves,
    tag_liquidity_dry_ups,
    tag_volume_surges,
)
from crypto_pipeline.features.orderbook import add_book_metrics
from crypto_pipeline.features.resample import bars_per_window, resample_trades_to_bars
from crypto_pipeline.features.volatility import add_realized_volatility
from crypto_pipeline.transform.schema import TRADE_SCHEMA


def _trades(rows: list[dict]) -> pl.DataFrame:
    defaults = {"exchange": "binance", "market_type": "spot", "symbol": "BTC-USDT", "is_liquidation": False}
    full_rows = [{**defaults, **row} for row in rows]
    return pl.DataFrame(full_rows, schema=TRADE_SCHEMA)


def test_bars_per_window():
    assert bars_per_window("1s", "1m") == 60
    assert bars_per_window("1s", "5m") == 300
    assert bars_per_window("1m", "5m") == 5
    assert bars_per_window("5m", "1m") is None  # window smaller than bar interval


def test_resample_trades_to_bars_single_bar_ohlcv():
    base_us = 1_704_067_200_000_000  # 2024-01-01T00:00:00Z, microseconds
    trades = _trades(
        [
            {"trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": base_us},
            {"trade_id": "2", "price": 110.0, "quantity": 2.0, "quote_quantity": 220.0, "side": "sell", "timestamp_utc": base_us + 500_000},
            {"trade_id": "3", "price": 105.0, "quantity": 1.0, "quote_quantity": 105.0, "side": "buy", "timestamp_utc": base_us + 900_000},
        ]
    )

    bars = resample_trades_to_bars(trades, timeframe="1s")

    assert bars.height == 1
    row = bars.row(0, named=True)
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 100.0
    assert row["close"] == 105.0
    assert row["volume_base"] == 4.0
    assert row["volume_quote"] == 425.0
    assert row["trade_count"] == 3
    assert row["buy_volume_base"] == 2.0
    assert row["sell_volume_base"] == 2.0
    assert row["volume_delta"] == 0.0
    expected_vwap = (100 * 1 + 110 * 2 + 105 * 1) / 4
    assert math.isclose(row["vwap"], expected_vwap)


def test_resample_trades_to_bars_splits_across_bars_and_symbols():
    base_us = 1_704_067_200_000_000
    trades = _trades(
        [
            {"trade_id": "1", "price": 100.0, "quantity": 1.0, "quote_quantity": 100.0, "side": "buy", "timestamp_utc": base_us},
            {"trade_id": "2", "price": 101.0, "quantity": 1.0, "quote_quantity": 101.0, "side": "buy", "timestamp_utc": base_us + 1_000_000},
            {
                "trade_id": "3",
                "price": 50.0,
                "quantity": 1.0,
                "quote_quantity": 50.0,
                "side": "buy",
                "timestamp_utc": base_us,
                "symbol": "ETH-USDT",
            },
        ]
    )

    bars = resample_trades_to_bars(trades, timeframe="1s")

    assert bars.height == 3  # two 1s bars for BTC-USDT, one for ETH-USDT
    assert set(bars["symbol"]) == {"BTC-USDT", "ETH-USDT"}


def test_add_realized_volatility_zero_when_flat():
    bars = pl.DataFrame(
        {
            "exchange": ["binance"] * 3,
            "symbol": ["BTC-USDT"] * 3,
            "close": [100.0, 100.0, 100.0],
        }
    )
    result = add_realized_volatility(bars, window_bars=3, column_name="rv")
    assert result["rv"].fill_null(0.0).to_list() == [0.0, 0.0, 0.0]


def test_add_realized_volatility_nonzero_on_move():
    bars = pl.DataFrame(
        {
            "exchange": ["binance"] * 2,
            "symbol": ["BTC-USDT"] * 2,
            "close": [100.0, 110.0],
        }
    )
    result = add_realized_volatility(bars, window_bars=2, column_name="rv")
    expected = abs(math.log(110.0 / 100.0))
    assert math.isclose(result["rv"][1], expected, rel_tol=1e-9)


def test_add_book_metrics_spread_and_depth():
    snapshots = pl.DataFrame(
        {
            "bid_price_1": [100.0],
            "bid_size_1": [1.0],
            "bid_price_2": [99.0],
            "bid_size_2": [10.0],
            "ask_price_1": [100.2],
            "ask_size_1": [1.0],
            "ask_price_2": [102.0],
            "ask_size_2": [10.0],
        }
    )

    result = add_book_metrics(snapshots, depth_levels=2)
    row = result.row(0, named=True)

    assert math.isclose(row["mid_price"], 100.1)
    assert math.isclose(row["spread"], 0.2)
    assert row["spread_bps"] > 0
    # Level 2 bid (99.0) is >1% below mid (100.1 * 0.99 ~= 99.099) -> excluded from 1% depth
    assert math.isclose(row["bid_depth_usd_1pct"], 100.0 * 1.0)
    # Level 2 ask (102.0) is >1% but within 2% of mid (100.1) -> included only in 2% depth
    assert row["ask_depth_usd_2pct"] > row["ask_depth_usd_1pct"]


def test_tag_flash_moves():
    bars = pl.DataFrame(
        {
            "exchange": ["binance"] * 3,
            "symbol": ["BTC-USDT"] * 3,
            "close": [100.0, 101.0, 200.0],
        }
    )
    result = tag_flash_moves(bars, EventThresholds(flash_move_pct=5.0, flash_move_window_bars=1))
    assert result["is_flash_move"].to_list() == [False, False, True]


def test_tag_volume_surges():
    bars = pl.DataFrame(
        {
            "exchange": ["binance"] * 6,
            "symbol": ["BTC-USDT"] * 6,
            "volume_base": [10.0, 11.0, 9.0, 10.0, 10.0, 500.0],
        }
    )
    result = tag_volume_surges(bars, EventThresholds(volume_surge_zscore=3.0, volume_surge_window_bars=5))
    assert result["is_volume_surge"][-1] is True


def test_tag_liquidity_dry_ups():
    book = pl.DataFrame({"spread_bps": [5.0, 80.0]})
    result = tag_liquidity_dry_ups(book, EventThresholds(spread_blowout_bps=50.0))
    assert result["is_liquidity_dry_up"].to_list() == [False, True]
