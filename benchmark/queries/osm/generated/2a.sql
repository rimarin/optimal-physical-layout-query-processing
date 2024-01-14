-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -63.394292132061494 AND min_lon < -63.33481782937538 AND
      max_lon > -174.8425733413466 AND max_lon < -31.136007398725496