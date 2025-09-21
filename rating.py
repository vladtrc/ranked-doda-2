import duckdb

import rating_complex
import rating_simple25

algs = {
    '25': rating_simple25.calculate_ranked_mmr,
    'complex': rating_complex.calculate_ranked_mmr
}

mode = 'complex'

def calculate_ranked_mmr(conn: duckdb.DuckDBPyConnection) -> None:
    f = algs[mode]
    f(conn)