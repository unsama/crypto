"""Task 1.1: Binance public archive harvester (data.binance.vision).

Downloads monthly trade / aggTrade archives, verifying each file against
its published `.CHECKSUM` sidecar. Also supports listing the underlying
S3 bucket to discover which archives currently exist for a symbol.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from xml.etree import ElementTree

import aiohttp

from crypto_pipeline.config import RAW_DIR
from crypto_pipeline.ingestion.download_manager import DownloadManager

logger = logging.getLogger(__name__)

ARCHIVE_BASE_URL = "https://data.binance.vision"
BUCKET_LISTING_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
S3_XML_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

# e.g. "spot", "futures/um", "futures/cm"
MarketType = str
# e.g. "trades", "aggTrades"
DataType = str


@dataclass(frozen=True)
class ArchiveFile:
    key: str  # full S3 object key, e.g. data/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-01.zip

    @property
    def url(self) -> str:
        return f"{ARCHIVE_BASE_URL}/{self.key}"

    @property
    def checksum_url(self) -> str:
        return f"{self.url}.CHECKSUM"

    @property
    def filename(self) -> str:
        return self.key.rsplit("/", 1)[-1]


def monthly_prefix(market: MarketType, symbol: str, data_type: DataType = "trades") -> str:
    return f"data/{market}/monthly/{data_type}/{symbol}/"


def build_monthly_key(
    market: MarketType, symbol: str, year: int, month: int, data_type: DataType = "trades"
) -> str:
    return f"data/{market}/monthly/{data_type}/{symbol}/{symbol}-{data_type}-{year:04d}-{month:02d}.zip"


def month_range(start: date, end: date) -> list[tuple[int, int]]:
    """Inclusive (year, month) pairs from start to end (day-of-month ignored)."""
    months = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


async def list_archive_files(session: aiohttp.ClientSession, prefix: str) -> list[ArchiveFile]:
    """List all objects under `prefix` in the Binance public data bucket (paginated)."""
    files: list[ArchiveFile] = []
    marker: str | None = None
    while True:
        params = {"prefix": prefix}
        if marker:
            params["marker"] = marker
        async with session.get(BUCKET_LISTING_URL, params=params) as resp:
            resp.raise_for_status()
            body = await resp.text()

        root = ElementTree.fromstring(body)
        keys = [el.text for el in root.iter(f"{S3_XML_NS}Key") if el.text]
        files.extend(ArchiveFile(key=k) for k in keys if not k.endswith(".CHECKSUM"))

        is_truncated = root.findtext(f"{S3_XML_NS}IsTruncated") == "true"
        if not is_truncated or not keys:
            break
        marker = keys[-1]

    return files


async def fetch_checksum(session: aiohttp.ClientSession, archive_file: ArchiveFile) -> str | None:
    """Fetch and parse the `<sha256>  <filename>` .CHECKSUM sidecar, if published."""
    async with session.get(archive_file.checksum_url) as resp:
        if resp.status == 404:
            return None
        resp.raise_for_status()
        text = await resp.text()
    return text.strip().split()[0]


class BinanceArchiveHarvester:
    """Downloads monthly trade/aggTrade archives from data.binance.vision."""

    def __init__(self, download_manager: DownloadManager, dest_root: Path = RAW_DIR / "binance") -> None:
        self._dm = download_manager
        self._dest_root = dest_root

    async def harvest_monthly(
        self,
        market: MarketType,
        symbol: str,
        start: date,
        end: date,
        data_type: DataType = "trades",
        verify_checksum: bool = True,
    ) -> list[Path]:
        """Download every monthly archive for `symbol` between `start` and `end` (inclusive months)."""
        archive_files = [
            ArchiveFile(key=build_monthly_key(market, symbol, y, m, data_type))
            for y, m in month_range(start, end)
        ]

        results = await asyncio.gather(
            *(self._harvest_one(af, verify_checksum) for af in archive_files),
            return_exceptions=True,
        )

        paths: list[Path] = []
        for archive_file, result in zip(archive_files, results):
            if isinstance(result, Exception):
                logger.error("failed to download %s: %s", archive_file.url, result)
                continue
            paths.append(result)
        return paths

    async def _harvest_one(self, archive_file: ArchiveFile, verify_checksum: bool) -> Path:
        expected_sha256 = None
        if verify_checksum:
            expected_sha256 = await fetch_checksum(self._dm.session, archive_file)

        dest_path = self._dest_root / archive_file.key.removeprefix("data/")
        result = await self._dm.download(archive_file.url, dest_path, expected_sha256=expected_sha256)
        logger.info(
            "%s %s (%s bytes)",
            "cached" if result.skipped else "downloaded",
            result.path,
            result.bytes_downloaded,
        )
        return result.path

    async def discover(self, market: MarketType, symbol: str, data_type: DataType = "trades") -> list[ArchiveFile]:
        """List all currently published monthly archives for `symbol` without downloading."""
        prefix = monthly_prefix(market, symbol, data_type)
        return await list_archive_files(self._dm.session, prefix)
