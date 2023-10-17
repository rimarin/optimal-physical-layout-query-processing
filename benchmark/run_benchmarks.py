import duckdb
import os

SCALE_FACTOR = 1
DATASETS_FOLDER = '../datasets/'
DATASETS = ['taxi', 'tpch']

dataset_folder = DATASETS_FOLDER + DATASETS[1]
files_already_imported = os.path.isfile(f"{DATASETS_FOLDER}/tpch/customer.parquet")
if not files_already_imported:
    os.system(f"python3 ../datasets/tpch/import_tpch.py {SCALE_FACTOR}")

# TPC-H
duckdb.sql(f"IMPORT DATABASE '{dataset_folder}'")

query = ("select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority "
         "from customer, orders, lineitem "
         "where "
         "c_mktsegment = \'AUTOMOBILE\' "
         "and c_custkey = o_custkey "
         "and l_orderkey = o_orderkey "
         "and o_orderdate < date \'1995-03-21\' "
         "and l_shipdate > date \'1995-03-21\' "
         "group by l_orderkey, o_orderdate, o_shippriority,"
         "order by revenue desc, o_orderdate")

res = duckdb.sql(f"EXPLAIN ANALYZE {query}")
print(res)
# duckdb.sql("EXPLAIN ANALYZE SELECT * FROM read_parquet('../test/FixedGrid/*.parquet') WHERE month < 6 and day > 15")
