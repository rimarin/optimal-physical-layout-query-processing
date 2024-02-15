-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -138.63850535600972 AND min_lon < 156.36660329592473 AND
      max_lon > 60.04782687333187 AND max_lon < 74.6375658591403