"""Phase 1/2 CLI: download raw archives, and normalize them into the
unified trades_tick schema.

Examples:
    python -m crypto_pipeline.cli binance --market spot --symbol BTCUSDT \\
        --start 2024-01 --end 2024-03

    python -m crypto_pipeline.cli bybit --symbol BTCUSD \\
        --start 2024-01-01 --end 2024-01-07

    python -m crypto_pipeline.cli kraken --pair XBTUSD \\
        --start 2024-01-01 --end 2024-01-02

    python -m crypto_pipeline.cli normalize binance --market spot \\
        --symbol BTCUSDT --data-type trades \\
        --path data/bronze/raw/binance/spot/monthly/trades/BTCUSDT/BTCUSDT-trades-2024-01.zip
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, datetime
from pathlib import Path

import aiohttp

from crypto_pipeline.ingestion.binance import BinanceArchiveHarvester
from crypto_pipeline.ingestion.bybit import BybitArchiveFetcher
from crypto_pipeline.ingestion.download_manager import DownloadManager
from crypto_pipeline.ingestion.kraken import KrakenTradesFetcher
from crypto_pipeline.transform.pipeline import (
    default_dest_path,
    normalize_binance_file,
    normalize_bybit_file,
    normalize_kraken_pair,
    write_normalized,
)


def _parse_month(value: str) -> date:
    return datetime.strptime(value, "%Y-%m").date().replace(day=1)


def _parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


async def run_binance(args: argparse.Namespace) -> None:
    async with DownloadManager(concurrency=args.concurrency) as dm:
        harvester = BinanceArchiveHarvester(dm)
        paths = await harvester.harvest_monthly(
            market=args.market,
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            data_type=args.data_type,
        )
        print(f"binance: {len(paths)} file(s) ready")


async def run_bybit(args: argparse.Namespace) -> None:
    async with DownloadManager(concurrency=args.concurrency) as dm:
        fetcher = BybitArchiveFetcher(dm)
        paths = await fetcher.fetch_daily(symbol=args.symbol, start=args.start, end=args.end)
        print(f"bybit: {len(paths)} file(s) ready")


async def run_kraken(args: argparse.Namespace) -> None:
    start_ns = int(datetime.combine(args.start, datetime.min.time()).timestamp() * 1_000_000_000)
    end_ns = int(datetime.combine(args.end, datetime.min.time()).timestamp() * 1_000_000_000)
    async with aiohttp.ClientSession() as session:
        fetcher = KrakenTradesFetcher(session, requests_per_second=args.rps)
        pages = await fetcher.fetch_range(pair=args.pair, start_ns=start_ns, end_ns=end_ns)
        total_trades = sum(p.trade_count for p in pages)
        print(f"kraken: {len(pages)} page(s), {total_trades} trade(s)")


async def run_normalize_binance(args: argparse.Namespace) -> None:
    df = normalize_binance_file(args.path, market=args.market, symbol=args.symbol, data_type=args.data_type)
    dest = args.out or default_dest_path("binance", args.symbol, args.path)
    write_normalized(df, dest)
    print(f"normalize binance: {df.height} row(s) -> {dest}")


async def run_normalize_bybit(args: argparse.Namespace) -> None:
    df = normalize_bybit_file(args.path, symbol=args.symbol, market=args.market)
    dest = args.out or default_dest_path("bybit", args.symbol, args.path)
    write_normalized(df, dest)
    print(f"normalize bybit: {df.height} row(s) -> {dest}")


async def run_normalize_kraken(args: argparse.Namespace) -> None:
    df = normalize_kraken_pair(args.path, pair=args.pair)
    dest = args.out or default_dest_path("kraken", args.pair, args.path)
    write_normalized(df, dest)
    print(f"normalize kraken: {df.height} row(s) -> {dest}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crypto_pipeline", description="Phase 1/2 ingestion + normalization CLI")
    parser.add_argument("--concurrency", type=int, default=8)
    subparsers = parser.add_subparsers(dest="exchange", required=True)

    binance_p = subparsers.add_parser("binance", help="Download monthly archives from data.binance.vision")
    binance_p.add_argument("--market", default="spot", choices=["spot", "futures/um", "futures/cm"])
    binance_p.add_argument("--symbol", required=True, help="e.g. BTCUSDT")
    binance_p.add_argument("--data-type", default="trades", choices=["trades", "aggTrades"])
    binance_p.add_argument("--start", type=_parse_month, required=True, help="YYYY-MM")
    binance_p.add_argument("--end", type=_parse_month, required=True, help="YYYY-MM")
    binance_p.set_defaults(func=run_binance)

    bybit_p = subparsers.add_parser("bybit", help="Download daily archives from public.bybit.com")
    bybit_p.add_argument("--symbol", required=True, help="e.g. BTCUSD")
    bybit_p.add_argument("--start", type=_parse_day, required=True, help="YYYY-MM-DD")
    bybit_p.add_argument("--end", type=_parse_day, required=True, help="YYYY-MM-DD")
    bybit_p.set_defaults(func=run_bybit)

    kraken_p = subparsers.add_parser("kraken", help="Paginate historical trades from Kraken's REST API")
    kraken_p.add_argument("--pair", required=True, help="e.g. XBTUSD")
    kraken_p.add_argument("--start", type=_parse_day, required=True, help="YYYY-MM-DD")
    kraken_p.add_argument("--end", type=_parse_day, required=True, help="YYYY-MM-DD")
    kraken_p.add_argument("--rps", type=float, default=1.0, help="requests per second")
    kraken_p.set_defaults(func=run_kraken)

    normalize_p = subparsers.add_parser("normalize", help="Normalize a downloaded raw archive into the unified schema")
    normalize_sub = normalize_p.add_subparsers(dest="normalize_exchange", required=True)

    norm_binance_p = normalize_sub.add_parser("binance")
    norm_binance_p.add_argument("--path", type=Path, required=True, help="path to the downloaded .zip archive")
    norm_binance_p.add_argument("--market", default="spot", choices=["spot", "futures/um", "futures/cm"])
    norm_binance_p.add_argument("--symbol", required=True, help="e.g. BTCUSDT")
    norm_binance_p.add_argument("--data-type", default="trades", choices=["trades", "aggTrades"])
    norm_binance_p.add_argument("--out", type=Path, default=None, help="output .parquet path")
    norm_binance_p.set_defaults(func=run_normalize_binance)

    norm_bybit_p = normalize_sub.add_parser("bybit")
    norm_bybit_p.add_argument("--path", type=Path, required=True, help="path to the downloaded .csv.gz archive")
    norm_bybit_p.add_argument("--symbol", required=True, help="e.g. BTCUSD")
    norm_bybit_p.add_argument("--market", default="linear", choices=["linear", "inverse"])
    norm_bybit_p.add_argument("--out", type=Path, default=None, help="output .parquet path")
    norm_bybit_p.set_defaults(func=run_normalize_bybit)

    norm_kraken_p = normalize_sub.add_parser("kraken")
    norm_kraken_p.add_argument("--path", type=Path, required=True, help="directory of raw *.json trade pages")
    norm_kraken_p.add_argument("--pair", required=True, help="e.g. XBTUSD")
    norm_kraken_p.add_argument("--out", type=Path, default=None, help="output .parquet path")
    norm_kraken_p.set_defaults(func=run_normalize_kraken)

    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
