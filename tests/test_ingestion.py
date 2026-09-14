from datetime import date
from pathlib import Path

import pytest

from crypto_pipeline.ingestion.binance import build_monthly_key, month_range
from crypto_pipeline.ingestion.bybit import day_range
from crypto_pipeline.ingestion.download_manager import DownloadManager, _sha256_file


def test_month_range_single_month():
    assert month_range(date(2024, 1, 15), date(2024, 1, 20)) == [(2024, 1)]


def test_month_range_spans_year_boundary():
    assert month_range(date(2023, 11, 1), date(2024, 2, 1)) == [
        (2023, 11),
        (2023, 12),
        (2024, 1),
        (2024, 2),
    ]


def test_build_monthly_key():
    key = build_monthly_key("spot", "BTCUSDT", 2024, 1, "trades")
    assert key == "data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-01.zip"


def test_day_range_inclusive():
    days = day_range(date(2024, 1, 1), date(2024, 1, 3))
    assert days == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]


@pytest.mark.asyncio
async def test_download_skips_cached_file_with_matching_checksum(tmp_path: Path):
    dest = tmp_path / "file.zip"
    dest.write_bytes(b"hello world")
    expected_sha256 = _sha256_file(dest)

    async with DownloadManager() as dm:
        result = await dm.download("http://example.invalid/file.zip", dest, expected_sha256=expected_sha256)

    assert result.skipped is True
    assert result.bytes_downloaded == 0
