-- Q1
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -135.1294008623316 AND min_lon < -108.63453207428132
