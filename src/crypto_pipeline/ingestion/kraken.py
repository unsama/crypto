"""Task 1.2: Kraken historical trades fetcher (REST pagination).

Kraken exposes historical trades only through a paginated REST endpoint
(no bulk archive), so this fetcher walks it directly: fetch a page,
persist it as-is to the bronze layer, advance the cursor using the
`last` value Kraken returns, and repeat until the requested range is
exhausted. Pages are cached by their `since` cursor so re-running a
fetch is idempotent and resumable.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential_jitter

from crypto_pipeline.config import RAW_DIR

logger = logging.getLogger(__name__)

API_URL = "https://api.kraken.com/0/public/Trades"

RETRYABLE_EXCEPTIONS = (aiohttp.ClientError, asyncio.TimeoutError)


class KrakenApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class TradesPage:
    pair: str
    since_ns: int
    last_ns: int
    trade_count: int
    path: Path


class KrakenTradesFetcher:
    """Paginates `GET /0/public/Trades`, persisting each raw page to disk.

    `since` / `last` are Unix timestamps in nanoseconds, matching what
    Kraken's API returns in the `last` field of each response.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        dest_root: Path = RAW_DIR / "kraken",
        requests_per_second: float = 1.0,
        max_retries: int = 5,
    ) -> None:
        self._session = session
        self._dest_root = dest_root
        self._min_interval = 1.0 / requests_per_second
        self._max_retries = max_retries

    async def fetch_range(self, pair: str, start_ns: int, end_ns: int) -> list[TradesPage]:
        """Fetch all trades for `pair` with timestamps in [start_ns, end_ns)."""
        pages: list[TradesPage] = []
        since = start_ns
        dest_dir = self._dest_root / pair
        dest_dir.mkdir(parents=True, exist_ok=True)

        while since < end_ns:
            dest_path = dest_dir / f"{pair}_trades_since_{since}.json"

            if dest_path.exists():
                payload = json.loads(dest_path.read_text())
            else:
                payload = await self._fetch_page(pair, since)
                dest_path.write_text(json.dumps(payload))
                await asyncio.sleep(self._min_interval)

            result = payload["result"]
            trade_rows = next(v for k, v in result.items() if k != "last")
            last_ns = int(result["last"])

            pages.append(
                TradesPage(pair=pair, since_ns=since, last_ns=last_ns, trade_count=len(trade_rows), path=dest_path)
            )
            logger.info("kraken %s: since=%d -> last=%d (%d trades)", pair, since, last_ns, len(trade_rows))

            if last_ns <= since or not trade_rows:
                break  # no progress: exhausted available history
            since = last_ns

        return pages

    async def _fetch_page(self, pair: str, since_ns: int) -> dict:
        @retry(
            reraise=True,
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential_jitter(initial=1, max=60),
            retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        )
        async def _attempt() -> dict:
            async with self._session.get(API_URL, params={"pair": pair, "since": str(since_ns)}) as resp:
                resp.raise_for_status()
                payload = await resp.json()
            if payload.get("error"):
                raise KrakenApiError(f"{pair}: {payload['error']}")
            return payload

        return await _attempt()
