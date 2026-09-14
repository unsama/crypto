# Large-Scale Historical Cryptocurrency Data Pipeline & Dataset Generation

## 1. Project Overview & Objective
The goal of this project is to build an automated, scalable data extraction, processing, and storage pipeline to generate multi-year, multi-exchange cryptocurrency datasets containing tens to hundreds of millions of historical market records.

The pipeline must support:
- **Historical Bulk Ingestion**: Downloading public archives (e.g., Binance Public Data Lake / AWS S3 Open Data, Tardis archives, direct exchange historical endpoints).
- **Transformation & Normalization**: Standardizing schemas across exchanges (Binance, Bybit, OKX, Coinbase, Kraken, etc.).
- **High-Frequency Granularity**: Ingesting tick-by-tick trades, sub-second updates, 1-second/1-minute aggregates, and Level 2 (L2) order book depth snapshots.
- **Derived Feature Engineering**: Computing spreads, volume delta, VWAP, rolling realized volatility, book depth liquidity, and market event filters.
- **Efficient Storage**: Partitioned Apache Parquet / Delta Lake formats for fast analytical querying and sample exports (CSV/DuckDB).

---

## 2. Target Coverage & Exchange Matrix

| Exchange | Historical Reach | Trade-by-Trade (Tick) | Order Book Snapshots (L2) | OHLCV (1s / 1m) | Primary Source / Mechanism |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Binance (Spot & Futures)** | 2017 – Present | Yes (AggTrades / Trades) | Yes (BookDepth snapshots / diffs) | Yes (1s, 1m) | Binance Public Archive S3 / Data Dump (`data.binance.vision`) |
| **Bybit (Linear / Inverse)** | 2020 – Present | Yes (Execution tick data) | Yes (Orderbook Level 2) | Yes (1m) | Bybit Public Historical Data Archive |
| **Coinbase Pro / Advanced** | 2018 – Present | Yes (Match events) | Top of Book (BBO) / L2 | Yes (1m) | Public Trade Dumps / Exchange REST Pagination |
| **Kraken** | 2016 – Present | Yes (Historical Trades API) | L2 Book Snapshots | Yes (1m) | Kraken Historical REST & Public Data Dumps |
| **OKX** | 2019 – Present | Yes (Deals history) | L2 Book Depth | Yes (1s, 1m) | OKX Data History & Public Storage |

---

## 3. Data Schemas & Standardization

All raw ingested data must be normalized into standardized relational/columnar schemas.

### 3.1. Trade-Level (Tick) Schema (`trades_tick`)
- `exchange` (String): e.g., `'binance'`, `'bybit'`
- `market_type` (String): `'spot'`, `'linear_perpetual'`, `'inverse_perpetual'`
- `symbol` (String): Normalized symbol, e.g., `'BTC-USDT'`, `'ETH-USDT'`
- `timestamp_utc` (Timestamp / Int64 Microseconds): UTC execution timestamp
- `trade_id` (String / Int64): Native exchange trade ID
- `price` (Decimal64/Float64): Execution price
- `quantity` (Decimal64/Float64): Base asset volume executed
- `quote_quantity` (Decimal64/Float64): Quote asset volume ($ value = price * quantity)
- `side` (String): `'buy'` or `'sell'` (standardized taker direction: buyer-maker vs seller-maker)
- `is_liquidation` (Boolean): Flag for liquidation trades (where available in derivatives)

### 3.2. Order Book Snapshot Schema (`orderbook_l2_snapshots`)
- `exchange` (String)
- `symbol` (String)
- `timestamp_utc` (Timestamp / Int64 Microseconds)
- `bids` (Array / Nested Struct or Flattened Depth Levels 1 to 20): `price`, `size`
- `asks` (Array / Nested Struct or Flattened Depth Levels 1 to 20): `price`, `size`
- `bid_price_1`, `bid_size_1` (Best Bid / BBO)
- `ask_price_1`, `ask_size_1` (Best Ask / BBO)
- `spread` (Float64): `ask_price_1 - bid_price_1`
- `spread_bps` (Float64): `(spread / mid_price) * 10,000`
- `mid_price` (Float64): `(bid_price_1 + ask_price_1) / 2`
- `bid_depth_usd_1pct` (Float64): Total USD liquidity within 1% of mid price (bids)
- `ask_depth_usd_1pct` (Float64): Total USD liquidity within 1% of mid price (asks)

### 3.3. Resampled Aggregate Bars Schema (`ohlcv_bars_1s_1m`)
- `exchange` (String)
- `symbol` (String)
- `timeframe` (String): `'1s'`, `'1m'`, `'5m'`
- `bar_timestamp_utc` (Timestamp)
- `open` (Float64)
- `high` (Float64)
- `low` (Float64)
- `close` (Float64)
- `volume_base` (Float64): Total base volume
- `volume_quote` (Float64): Total quote volume (USD turnover)
- `trade_count` (Int64): Total number of discrete trades
- `buy_volume_base` (Float64): Taker buy volume
- `sell_volume_base` (Float64): Taker sell volume
- `volume_delta` (Float64): `buy_volume_base - sell_volume_base`
- `vwap` (Float64): Volume Weighted Average Price

---

## 4. System Architecture & Tech Stack

```
[ Public S3 Archives / Bulk APIs ]
              │
              ▼
   [ Ingestion Workers ]  (Python / aiohttp / boto3 / DuckDB)
              │
              ▼
   [ Staging / Bronze Layer ] (Raw compressed files: .csv.gz / .zip)
              │
              ▼
   [ ETL / Transformation Layer ] (Polars / PySpark / DuckDB)
   - Schema validation & deduplication
   - Timestamp alignment to UTC microseconds
   - Resampling & Indicator calculations
              │
              ▼
   [ Silver / Gold Analytical Layer ] (Partitioned Parquet / Delta Lake)
   - Partitioned by: exchange / symbol / year / month
              │
              ▼
   [ Export & Verification Suite ]
   - Data validation & gap detection report
   - Automated 24-hr sample generation (CSV / Parquet)
```

- **Runtime Environment**: Python 3.11+
- **High-Performance Data Engine**: `polars` (for local single-node multi-core processing) or `pyspark` / `duckdb`
- **Async HTTP & Archive Processing**: `aiohttp`, `aiofiles`, `zipfile`, `tarfile`
- **Storage Format**: Apache Parquet (Snappy or ZSTD compression)
- **Validation**: `pydantic` / `pandera` / Great Expectations for schema & null verification

---

## 5. Implementation Roadmap & Claude Code Tasks

> **Implementation status (2026-09-14)**: all tasks below are implemented; see [README.md](README.md) for the file-by-file breakdown and [CLAUDE.md](CLAUDE.md) for codebase structure/conventions. One data-availability gap, not a code gap: no exchange publishes real per-level L2 order books (raw bid/ask + BBO/spread) as a free bulk historical archive, so Task 3.2's spread/mid-price half and Task 5.1's negative-spread check are implemented against synthetic data only - see the "Order-book data" section of README.md for what's real (Binance's `bookDepth` percentage-depth archive) versus what would need a live capture or paid vendor.
>
> **Verified against real data (2026-09-14)**: a genuine BTCUSDT January 2024 monthly archive (52,549,865 trades, 3.7GB uncompressed) was normalized end-to-end, resampled to 44,640 gap-free 1-minute bars, passed the quality report with zero invalid rows, and is visualized in a published dashboard (link at the top of README.md). This run exposed and fixed a real out-of-memory bug in the normalize/resample path on memory-constrained machines - see README.md's Phase 2 section and CLAUDE.md's Conventions for the fix (chunked ingestion + lazy streaming). See README.md's "What's next" for suggested follow-on work.

### Phase 1: Ingestion Engine & Download Automation
- [x] **Task 1.1: Binance Public Archive Harvester**
  - Build asynchronous scraper/downloader for `data.binance.vision`.
  - Target prefixes: `data/spot/monthly/trades/`, `data/spot/monthly/aggTrades/`, `data/futures/um/monthly/trades/`.
  - Implement concurrent checksum verification (`.CHECKSUM` validation).
- [x] **Task 1.2: Bybit & Kraken Archive Fetchers**
  - Implement Bybit open-access historical execution parser.
  - Implement Kraken historical trade paginator using `https://api.kraken.com/0/public/Trades`.
- [x] **Task 1.3: Download Manager & Resiliency**
  - Implement resume capability, exponential backoff, rate-limiting, and local caching.

### Phase 2: Schema Normalization & High-Throughput Processing
- [x] **Task 2.1: Ingestion Workers & Polars Transformer**
  - Read raw daily/monthly `.zip` / `.csv.gz` streams directly into Polars streaming engine without uncompressing to disk.
  - Apply standard unified schema across exchanges.
- [x] **Task 2.2: Data Cleaning & Deduplication**
  - Detect out-of-order timestamps and sort deterministically.
  - Deduplicate trades based on `(exchange, symbol, trade_id)`.
  - Fix edge-case numeric overflows and scientific notations.

### Phase 3: Resampling & Feature Generation
- [x] **Task 3.1: Resampling Engine (Tick -> 1-Second & 1-Minute Bars)**
  - Implement high-performance resampling generating OHLC, volume, and tick count.
  - Calculate buyer/seller trade imbalance (`volume_delta`).
  - Calculate instantaneous VWAP: `sum(price * volume) / sum(volume)`.
- [x] **Task 3.2: Order Book Metrics & Derived Indicators** — realized volatility and percentage-bucketed depth (Binance `bookDepth`) run against real data; bid-ask spread/spread bps need real per-level BBO data, which doesn't exist as a free bulk archive anywhere (implemented and tested against synthetic snapshots only).
  - Compute rolling 1-minute and 5-minute Realized Volatility ($\sigma = \sqrt{\sum r_t^2}$).
  - Calculate bid-ask spreads, spread bps, and cumulative market depth at $\pm 1\%$ and $\pm 2\%$.
- [x] **Task 3.3: Specific Market-Event & Filter Engine**
  - Build configurable rule engine to tag market events:
    - Flash crashes / spikes (price change $> X\%$ in $T$ seconds).
    - Liquidity dry-ups (spread blowouts $> Y$ bps) — same missing-BBO-data caveat as Task 3.2.
    - Volume surge anomalies ($Z$-score $> 3.0$).

### Phase 4: Storage Optimization & Partitioning
- [x] **Task 4.1: Parquet Partitioning Strategy**
  - Write output files using standard hive partitioning:
    `output_lake/trades/exchange={exchange}/symbol={symbol}/year={YYYY}/month={MM}/data.parquet`
  - Target optimal row-group sizing (128 MB to 512 MB) with ZSTD level 3 compression.
- [x] **Task 4.2: DuckDB / SQLite Query Interface**
  - Provide ready-to-query scripts for instant local SQL exploration across partitioned Parquet directories without database loading overhead.

### Phase 5: QA, Validation & Client Sample Packaging
- [x] **Task 5.1: Data Quality & Completeness Auditor**
  - Run continuous timestamp gap detection (flagging intervals with zero trades during open market hours).
  - Check for zero-volume or negative-spread anomalies — negative-spread check needs real BBO data (see Task 3.2 caveat).
  - Generate automated summary markdown report (row counts, date ranges, min/max prices).
- [x] **Task 5.2: Sample Exporter**
  - CLI command to slice and export a standardized 24-hour verification package for any pair/exchange in CSV and Parquet formats.

---

## 6. Verification & Quality Acceptance Criteria

1. **Zero Data Corruption**: 100% of rows have non-null timestamps, strictly positive prices, and positive quantities.
2. **Deterministic Partitioning**: Re-running any pipeline batch yields bit-identical Parquet files.
3. **High Throughput**: Capable of processing at least 1,000,000 tick records/second per CPU core using Polars streaming.
4. **Sample Ready**: Immediate generation of sample datasets for evaluation upon client request.
