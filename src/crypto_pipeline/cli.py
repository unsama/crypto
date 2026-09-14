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

    python -m crypto_pipeline.cli resample --timeframe 1m \\
        --path "data/gold/trades/exchange=binance/symbol=BTC-USDT/**/data.parquet"

    python -m crypto_pipeline.cli query --sql "SELECT COUNT(*) FROM trades"

    python -m crypto_pipeline.cli report --out data/gold/quality_report.md

    python -m crypto_pipeline.cli sample-export --exchange binance --symbol BTC-USDT \\
        --start 2024-01-01 --out data/samples
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import aiohttp
import polars as pl

from crypto_pipeline.config import GOLD_DIR
from crypto_pipeline.features.events import tag_bar_events
from crypto_pipeline.features.resample import bars_per_window, resample_trades_to_bars
from crypto_pipeline.features.volatility import add_realized_volatility
from crypto_pipeline.ingestion.binance import BinanceArchiveHarvester
from crypto_pipeline.ingestion.bybit import BybitArchiveFetcher
from crypto_pipeline.ingestion.download_manager import DownloadManager
from crypto_pipeline.ingestion.kraken import KrakenTradesFetcher
from crypto_pipeline.qa.sample_export import export_sample
from crypto_pipeline.qa.summary import generate_summary_report
from crypto_pipeline.storage.partition import write_hive_partitioned
from crypto_pipeline.storage.query import connect as connect_lake
from crypto_pipeline.transform.binance_bookdepth import read_binance_book_depth
from crypto_pipeline.transform.pipeline import (
    normalize_binance_file,
    normalize_binance_file_chunks,
    normalize_bybit_file,
    normalize_kraken_pair,
    write_normalized,
)
from crypto_pipeline.transform.symbols import normalize_symbol

TRADE_MERGE_KEY = ["exchange", "symbol", "trade_id"]
BAR_MERGE_KEY = ["exchange", "symbol", "timeframe", "bar_timestamp_utc"]
BOOK_DEPTH_MERGE_KEY = ["exchange", "symbol", "timestamp_utc", "percentage"]


def _parse_month(value: str) -> date:
    return datetime.strptime(value, "%Y-%m").date().replace(day=1)


def _parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_utc_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


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


async def run_binance_bookdepth(args: argparse.Namespace) -> None:
    async with DownloadManager(concurrency=args.concurrency) as dm:
        harvester = BinanceArchiveHarvester(dm)
        paths = await harvester.harvest_daily(
            market="futures/um",
            symbol=args.symbol,
            start=args.start,
            end=args.end,
            data_type="bookDepth",
        )
        print(f"binance-bookdepth: {len(paths)} file(s) ready")


async def run_normalize_binance_bookdepth(args: argparse.Namespace) -> None:
    lf = read_binance_book_depth(args.path, symbol=args.symbol)
    df = lf.with_columns(pl.lit(normalize_symbol(args.symbol, "binance")).alias("symbol")).collect()

    if args.out is not None:
        write_normalized(df, args.out)
        print(f"normalize binance-bookdepth: {df.height} row(s) -> {args.out}")
        return

    written = write_hive_partitioned(df, GOLD_DIR, "book_depth_pct", "timestamp_utc", merge_key=BOOK_DEPTH_MERGE_KEY)
    print(f"normalize binance-bookdepth: {df.height} row(s) -> {len(written)} partition(s) under {GOLD_DIR / 'book_depth_pct'}")


def _write_trades(df: pl.DataFrame, out: Path | None, label: str) -> None:
    if out is not None:
        write_normalized(df, out)
        print(f"normalize {label}: {df.height} row(s) -> {out}")
        return
    written = write_hive_partitioned(df, GOLD_DIR, "trades", "timestamp_utc", merge_key=TRADE_MERGE_KEY)
    print(f"normalize {label}: {df.height} row(s) -> {len(written)} partition(s) under {GOLD_DIR / 'trades'}")


async def run_normalize_binance(args: argparse.Namespace) -> None:
    if args.out is not None:
        # Flat single-file export: kept as the original single eager
        # collect, since it's meant for small ad-hoc slices, not a full
        # high-volume monthly archive (use the default partitioned path
        # below for those - it processes in memory-bounded chunks).
        df = normalize_binance_file(args.path, market=args.market, symbol=args.symbol, data_type=args.data_type)
        _write_trades(df, args.out, "binance")
        return

    total_rows = 0
    total_parts = 0
    for i, chunk_df in enumerate(
        normalize_binance_file_chunks(args.path, market=args.market, symbol=args.symbol, data_type=args.data_type)
    ):
        written = write_hive_partitioned(
            chunk_df, GOLD_DIR, "trades", "timestamp_utc", part_name=f"{args.path.stem}-{i:04d}"
        )
        total_rows += chunk_df.height
        total_parts += len(written)
        print(f"normalize binance: chunk {i} - {chunk_df.height:,} row(s) -> {len(written)} part file(s)")

    print(f"normalize binance: {total_rows:,} row(s) total -> {total_parts} part file(s) under {GOLD_DIR / 'trades'}")


async def run_normalize_bybit(args: argparse.Namespace) -> None:
    df = normalize_bybit_file(args.path, symbol=args.symbol, market=args.market)
    _write_trades(df, args.out, "bybit")


async def run_normalize_kraken(args: argparse.Namespace) -> None:
    df = normalize_kraken_pair(args.path, pair=args.pair)
    _write_trades(df, args.out, "kraken")


async def run_resample(args: argparse.Namespace) -> None:
    paths = sorted(Path(p) for p in glob.glob(args.path, recursive=True))
    if not paths:
        raise SystemExit(f"no files matched {args.path!r}")

    # Lazy scan, not eager read+concat: a full month of a high-volume pair
    # is 50M+ trade rows, too large to materialize before resampling on a
    # memory-constrained machine. Only the much smaller bar output below
    # is ever collected.
    lf = pl.scan_parquet(paths)
    bars = resample_trades_to_bars(lf, timeframe=args.timeframe)

    for window in ("1m", "5m"):
        n = bars_per_window(args.timeframe, window)
        if n is not None:
            bars = add_realized_volatility(bars, n, f"realized_vol_{window}")
        else:
            logging.getLogger(__name__).info(
                "skipping realized_vol_%s: %s bars don't divide evenly into a %s window", window, args.timeframe, window
            )

    bars = tag_bar_events(bars)

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        bars.write_parquet(args.out, compression="zstd", compression_level=3)
        print(f"resample: {bars.height} bar(s) -> {args.out}")
        return

    written = write_hive_partitioned(bars, GOLD_DIR, "bars", "bar_timestamp_utc", merge_key=BAR_MERGE_KEY)
    print(f"resample: {bars.height} bar(s) -> {len(written)} partition(s) under {GOLD_DIR / 'bars'}")


async def run_query(args: argparse.Namespace) -> None:
    con = connect_lake(args.root)
    con.sql(args.sql).show(max_rows=args.limit)


async def run_report(args: argparse.Namespace) -> None:
    con = connect_lake(args.root)
    report = generate_summary_report(con)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(report)
        print(f"report: wrote -> {args.out}")
    else:
        print(report)


async def run_sample_export(args: argparse.Namespace) -> None:
    con = connect_lake(args.root)
    result = export_sample(con, exchange=args.exchange, symbol=args.symbol, start=args.start, out_dir=args.out, hours=args.hours)
    print(f"sample-export: {result.row_count} row(s) -> {result.csv_path}, {result.parquet_path}")


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

    bookdepth_p = subparsers.add_parser(
        "binance-bookdepth",
        help="Download daily futures bookDepth (percentage-bucketed order-book depth) archives from data.binance.vision",
    )
    bookdepth_p.add_argument("--symbol", required=True, help="e.g. BTCUSDT (futures/um only)")
    bookdepth_p.add_argument("--start", type=_parse_day, required=True, help="YYYY-MM-DD")
    bookdepth_p.add_argument("--end", type=_parse_day, required=True, help="YYYY-MM-DD")
    bookdepth_p.set_defaults(func=run_binance_bookdepth)

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

    norm_bookdepth_p = normalize_sub.add_parser("binance-bookdepth")
    norm_bookdepth_p.add_argument("--path", type=Path, required=True, help="path to the downloaded bookDepth .zip archive")
    norm_bookdepth_p.add_argument("--symbol", required=True, help="e.g. BTCUSDT")
    norm_bookdepth_p.add_argument("--out", type=Path, default=None, help="output .parquet path")
    norm_bookdepth_p.set_defaults(func=run_normalize_binance_bookdepth)

    resample_p = subparsers.add_parser(
        "resample", help="Resample normalized trades into OHLCV bars with volatility and event tags"
    )
    resample_p.add_argument("--path", required=True, help="glob of normalized .parquet trade files to resample")
    resample_p.add_argument("--timeframe", default="1m", choices=["1s", "1m", "5m"])
    resample_p.add_argument(
        "--out", type=Path, default=None, help="output .parquet path (default: hive-partitioned under data/gold/bars)"
    )
    resample_p.set_defaults(func=run_resample)

    query_p = subparsers.add_parser("query", help="Run a SQL query against the partitioned Parquet lake via DuckDB")
    query_p.add_argument("--root", type=Path, default=GOLD_DIR, help="lake root (default: data/gold)")
    query_p.add_argument("--sql", required=True, help='e.g. "SELECT COUNT(*) FROM trades"')
    query_p.add_argument("--limit", type=int, default=100, help="max rows to print")
    query_p.set_defaults(func=run_query)

    report_p = subparsers.add_parser("report", help="Generate a data-quality summary markdown report")
    report_p.add_argument("--root", type=Path, default=GOLD_DIR, help="lake root (default: data/gold)")
    report_p.add_argument("--out", type=Path, default=None, help="write the report here instead of stdout")
    report_p.set_defaults(func=run_report)

    sample_p = subparsers.add_parser("sample-export", help="Export a verification sample (CSV + Parquet)")
    sample_p.add_argument("--root", type=Path, default=GOLD_DIR, help="lake root (default: data/gold)")
    sample_p.add_argument("--exchange", required=True)
    sample_p.add_argument("--symbol", required=True, help="normalized symbol, e.g. BTC-USDT")
    sample_p.add_argument("--start", type=_parse_utc_datetime, required=True, help="YYYY-MM-DD (UTC)")
    sample_p.add_argument("--hours", type=int, default=24)
    sample_p.add_argument("--out", type=Path, required=True, help="output directory")
    sample_p.set_defaults(func=run_sample_export)

    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
