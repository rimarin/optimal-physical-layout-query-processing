# Generate the TPC-H benchmark database from DuckDB
# https://duckdb.org/docs/extensions/tpch.html
# https://duckdb.org/docs/sql/statements/export.html
import duckdb
import sys


def import_tpch(scale=1):
    # Install/load the tpch extension
    duckdb.sql('INSTALL tpch')
    duckdb.sql('LOAD tpch')

    # Generate data for scale factor 1, use:
    duckdb.sql(f'CALL dbgen(sf={scale})')

    # Export the entire database to parquet files
    duckdb.sql('EXPORT DATABASE \'tpch\' (FORMAT PARQUET)')


def __init__():
    if sys.argv[1]:
        import_tpch(sys.argv[1])
