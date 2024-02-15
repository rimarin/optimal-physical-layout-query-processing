-- Q1
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 175.90895336227538 AND PULocationID < 225.97491758435046
