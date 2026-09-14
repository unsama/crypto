"""Shared paths and settings for the data pipeline."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = PROJECT_ROOT / "data"

BRONZE_DIR = DATA_ROOT / "bronze"
SILVER_DIR = DATA_ROOT / "silver"
GOLD_DIR = DATA_ROOT / "gold"

# Raw archive downloads land here, namespaced by exchange, before ETL.
RAW_DIR = BRONZE_DIR / "raw"

DEFAULT_CONCURRENCY = 8
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 5
