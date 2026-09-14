"""Task 1.2: Bybit public archive fetcher (public.bybit.com).

Bybit publishes daily gzip'd execution/trade history with no auth and no
published checksums, so this simply builds the daily URL set and pulls
each file through the shared DownloadManager (which still gives us
resume, retry, and caching).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from crypto_pipeline.config import RAW_DIR
from crypto_pipeline.ingestion.download_manager import DownloadManager

logger = logging.getLogger(__name__)

ARCHIVE_BASE_URL = "https://public.bybit.com/trading"


@dataclass(frozen=True)
class DailyFile:
    symbol: str
    day: date

    @property
    def filename(self) -> str:
        return f"{self.symbol}{self.day.isoformat()}.csv.gz"

    @property
    def url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.symbol}/{self.filename}"


def day_range(start: date, end: date) -> list[date]:
    """Inclusive list of dates from start to end."""
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    return days


class BybitArchiveFetcher:
    """Downloads daily execution/trade archives from public.bybit.com."""

    def __init__(self, download_manager: DownloadManager, dest_root: Path = RAW_DIR / "bybit") -> None:
        self._dm = download_manager
        self._dest_root = dest_root

    async def fetch_daily(self, symbol: str, start: date, end: date) -> list[Path]:
        """Download every daily archive for `symbol` between `start` and `end` (inclusive)."""
        daily_files = [DailyFile(symbol=symbol, day=d) for d in day_range(start, end)]

        results = await asyncio.gather(
            *(self._fetch_one(df) for df in daily_files),
            return_exceptions=True,
        )

        paths: list[Path] = []
        for daily_file, result in zip(daily_files, results):
            if isinstance(result, Exception):
                logger.error("failed to download %s: %s", daily_file.url, result)
                continue
            paths.append(result)
        return paths

    async def _fetch_one(self, daily_file: DailyFile) -> Path:
        dest_path = self._dest_root / daily_file.symbol / daily_file.filename
        result = await self._dm.download(daily_file.url, dest_path)
        logger.info(
            "%s %s (%s bytes)",
            "cached" if result.skipped else "downloaded",
            result.path,
            result.bytes_downloaded,
        )
        return result.path
