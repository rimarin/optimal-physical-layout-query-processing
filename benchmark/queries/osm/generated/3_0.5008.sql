-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 41.590875199494405 AND min_lon < 100.64796423820974 AND
      max_lon > 49.03779746707093 AND max_lon < 54.081786454815756 AND
      min_lat > -51.50580367214468 AND min_lat < 76.36935043099291