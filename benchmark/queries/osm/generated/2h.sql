-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -155.18161804058064 AND min_lon < 54.50785976488595 AND
      max_lon > 32.27974435572409 AND max_lon < 110.61570509146549