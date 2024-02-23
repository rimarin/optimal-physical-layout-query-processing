### DuckDB configuration

Please note that DuckDB has to be explicitly configured to perform out-of-core processing.
This is done through 2 commands:

```
SET memory_limit='40GB';
SET temp_directory='/tmp';
```

The memory limit shall be set to a sensible value, ~80% of the estimated needed memory.
It may indeed happen that DuckDB crosses the specified threshold, due to the combinations of 
the operators, that leads to inaccurate memory usage estimation. 

### Dataset generation reference

Generate denormalized, scaled TPC-H dataset
```
LOAD tpch;
CALL dbgen(sf=100);
CREATE TABLE lineitem_denorm AS SELECT * FROM customer, lineitem, nation, orders, part, partsupp, region, supplier WHERE c_custkey = o_custkey AND n_nationkey = c_nationkey AND n_nationkey = s_nationkey AND o_orderkey = l_orderkey AND p_partkey = ps_partkey AND ps_partkey = l_partkey AND ps_suppkey = l_suppkey AND r_regionkey = n_regionkey AND s_suppkey = ps_suppkey;
COPY (SELECT * FROM lineitem_denorm) TO 'benchmark/datasets/tpch-sf100/no-partition/tpch-sf100.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072);
```

Convert multiple taxi data into one parquet file
```
CREATE TABLE taxi AS SELECT * FROM read_parquet('benchmark/datasets/taxi/original/*.parquet');
COPY (SELECT * FROM taxi) TO 'benchmark/datasets/taxi/no-partition/taxi.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072);
```

Convert multiple OSM data into one parquet file
```
CREATE TABLE osm AS SELECT * FROM read_parquet('benchmark/datasets/osm/original/*.parquet');
COPY (SELECT * FROM osm) TO 'benchmark/datasets/osm/no-partition/osm.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 131072);
```