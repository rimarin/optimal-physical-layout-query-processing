-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 3 AND PULocationID < 20 AND
      DOLocationID > 18 AND DOLocationID < 233
