-- Q1
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 4 AND PULocationID < 9
