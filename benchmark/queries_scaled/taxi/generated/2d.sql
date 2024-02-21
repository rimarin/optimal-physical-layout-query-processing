-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 31 AND PULocationID < 108 AND
      DOLocationID > 26 AND DOLocationID < 43
