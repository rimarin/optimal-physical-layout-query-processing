# Generate the TPC-H benchmark database from DuckDB
# https://duckdb.org/docs/extensions/tpch.html
# https://duckdb.org/docs/sql/statements/export.html
import duckdb

# Install/load the tpch extension
duckdb.sql('INSTALL tpch')
duckdb.sql('LOAD tpch')

# Generate data for scale factor 1, use:
duckdb.sql('CALL dbgen(sf=1)')

# Export the entire database to parquet files
duckdb.sql('EXPORT DATABASE \'tpch\' (FORMAT PARQUET)')
