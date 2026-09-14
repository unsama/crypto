"""Task 4.2: DuckDB query interface over the partitioned Parquet lake.

No loading step: DuckDB reads the partition files directly off disk via
glob + hive partitioning, so a query against millions of rows can prune
straight to the relevant exchange/symbol/year/month partitions.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def connect(root: Path) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with one view per dataset under `root`.

    Each immediate subdirectory of `root` (e.g. `root/trades`, `root/bars`)
    becomes a same-named view over every `data.parquet` partition file
    beneath it, with the `exchange`/`symbol`/`year`/`month` hive keys
    exposed as real columns.
    """
    con = duckdb.connect(database=":memory:")
    if root.exists():
        for dataset_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            # DuckDB's CREATE VIEW can't bind prepared-statement parameters, so
            # the glob is inlined as a literal (escaping any single quotes).
            glob_pattern = str(dataset_dir / "**" / "data.parquet").replace("'", "''")
            con.execute(
                f"CREATE OR REPLACE VIEW {dataset_dir.name} AS "
                f"SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning = true)"
            )
    return con
