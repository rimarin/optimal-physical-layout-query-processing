-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -128.16013285603907 AND min_lon < 36.31629386830198 AND
      max_lon > -100.17844155615553 AND max_lon < -86.32766217009295 AND
      min_lat > 7.59723556076564 AND min_lat < 41.6629908108649