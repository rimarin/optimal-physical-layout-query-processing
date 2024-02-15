-- Q1
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 84.1165026005387 AND min_lon < 100.06221466007327
