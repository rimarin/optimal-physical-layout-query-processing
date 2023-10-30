-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > 47.450070075677274 AND min_lon < 87.97443796045735 AND
      max_lon > 42.44887744740677 AND max_lon < 88.37589529565662 AND
      min_lat > 43.43331182308694 AND min_lat < 72.99114728850049