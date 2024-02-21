-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 191 AND PULocationID < 202 AND
      DOLocationID > 13 AND DOLocationID < 206
