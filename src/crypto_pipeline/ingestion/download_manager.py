"""Task 1.3: resilient, resumable, rate-limited download manager.

Shared by every exchange-specific fetcher (Binance, Bybit, Kraken, ...).
Handles: concurrency limiting, exponential backoff retries, resumable
partial downloads, local caching (skip already-verified files), and
optional SHA-256 checksum verification.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path

import aiofiles
import aiohttp
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1 MiB

RETRYABLE_EXCEPTIONS = (
    aiohttp.ClientError,
    asyncio.TimeoutError,
    ConnectionResetError,
)


@dataclass(frozen=True)
class DownloadResult:
    url: str
    path: Path
    skipped: bool  # True if a valid cached copy already existed
    bytes_downloaded: int


class ChecksumMismatchError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


class DownloadManager:
    """Async download manager with resume, retry, and caching.

    Usage:
        async with DownloadManager(concurrency=8) as dm:
            result = await dm.download(url, dest_path, expected_sha256=...)
    """

    def __init__(
        self,
        concurrency: int = 8,
        max_retries: int = 5,
        timeout_seconds: int = 60,
        requests_per_second: float | None = None,
    ) -> None:
        self._semaphore = asyncio.Semaphore(concurrency)
        self._max_retries = max_retries
        self._timeout = aiohttp.ClientTimeout(total=None, sock_connect=timeout_seconds, sock_read=timeout_seconds)
        self._min_interval = (1.0 / requests_per_second) if requests_per_second else 0.0
        self._last_request_ts = 0.0
        self._rate_lock = asyncio.Lock()
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "DownloadManager":
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("DownloadManager must be used as an async context manager")
        return self._session

    async def _throttle(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._rate_lock:
            now = asyncio.get_event_loop().time()
            wait_for = self._last_request_ts + self._min_interval - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
            self._last_request_ts = asyncio.get_event_loop().time()

    async def download(
        self,
        url: str,
        dest_path: Path,
        expected_sha256: str | None = None,
        force: bool = False,
    ) -> DownloadResult:
        """Download `url` to `dest_path`, resuming/retrying/caching as needed."""
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        if not force and dest_path.exists():
            if expected_sha256 is None or _sha256_file(dest_path) == expected_sha256:
                logger.debug("cache hit, skipping download: %s", dest_path)
                return DownloadResult(url=url, path=dest_path, skipped=True, bytes_downloaded=0)
            logger.warning("checksum mismatch on cached file, re-downloading: %s", dest_path)
            dest_path.unlink()

        async with self._semaphore:
            bytes_downloaded = await self._download_with_retry(url, dest_path)

        if expected_sha256 is not None:
            actual = _sha256_file(dest_path)
            if actual != expected_sha256:
                dest_path.unlink(missing_ok=True)
                raise ChecksumMismatchError(f"{url}: expected {expected_sha256}, got {actual}")

        return DownloadResult(url=url, path=dest_path, skipped=False, bytes_downloaded=bytes_downloaded)

    async def _download_with_retry(self, url: str, dest_path: Path) -> int:
        @retry(
            reraise=True,
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=1, max=60),
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
            before_sleep=lambda rs: logger.warning(
                "retrying %s (attempt %d/%d) after error: %s",
                url,
                rs.attempt_number,
                self._max_retries,
                rs.outcome.exception() if rs.outcome else None,
            ),
        )
        async def _attempt() -> int:
            return await self._fetch_to_file(url, dest_path)

        return await _attempt()

    async def _fetch_to_file(self, url: str, dest_path: Path) -> int:
        part_path = dest_path.with_suffix(dest_path.suffix + ".part")
        resume_from = part_path.stat().st_size if part_path.exists() else 0

        headers = {"Range": f"bytes={resume_from}-"} if resume_from else {}
        await self._throttle()

        async with self.session.get(url, headers=headers) as resp:
            if resp.status == 416:
                # Range not satisfiable: we already have the full file.
                part_path.rename(dest_path)
                return resume_from

            if resume_from and resp.status == 200:
                # Server ignored the Range request; restart from scratch.
                resume_from = 0
                mode = "wb"
            elif resp.status in (200, 206):
                resp.raise_for_status()
                mode = "ab" if resume_from and resp.status == 206 else "wb"
            else:
                resp.raise_for_status()
                mode = "wb"

            written = resume_from if mode == "ab" else 0
            async with aiofiles.open(part_path, mode) as f:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    await f.write(chunk)
                    written += len(chunk)

        part_path.rename(dest_path)
        return written

    async def download_many(
        self,
        items: list[tuple[str, Path, str | None]],
    ) -> list[DownloadResult]:
        """Download a batch of (url, dest_path, expected_sha256) concurrently."""
        tasks = [self.download(url, path, checksum) for url, path, checksum in items]
        return await asyncio.gather(*tasks)
