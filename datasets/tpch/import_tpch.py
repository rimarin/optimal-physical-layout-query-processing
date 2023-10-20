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

    # Denormalize database into a big, joined fact table lineitem
    # Size should approximately increase by x7 factor
    duckdb.sql('CREATE TABLE lineitem_denorm AS '
               'SELECT * '
               'FROM customer, lineitem, nation, orders, part, partsupp, region, supplier '
               'WHERE '
                   'c_custkey = o_custkey AND '
                   'n_nationkey = c_nationkey AND '
                   'n_nationkey = s_nationkey AND '
                   'o_orderkey = l_orderkey AND '
                   'p_partkey = ps_partkey AND '
                   'ps_partkey = l_partkey AND '
                   'ps_suppkey = l_suppkey AND '
                   'r_regionkey = n_regionkey AND '
                   's_suppkey = ps_suppkey;')

    # Export the entire table to a parquet file
    duckdb.sql('COPY (SELECT * FROM lineitem_denorm) TO \'lineitem.parquet\' (FORMAT PARQUET)')


def __init__():
    if sys.argv[1]:
        import_tpch(scale=sys.argv[1])
