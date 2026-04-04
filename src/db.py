"""
In-memory DuckDB singleton.
Call init_db() once at startup; then use get_conn() everywhere.
"""

import duckdb
from .data_parse import parse_dota_file
from .data_to_duckdb import load_matches_into_duckdb

_conn: duckdb.DuckDBPyConnection | None = None


def init_db(data_file: str = "data.txt") -> None:
    global _conn
    matches = parse_dota_file(data_file)
    _conn = load_matches_into_duckdb(matches)


def get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("DB not initialised – call init_db() first")
    return _conn
