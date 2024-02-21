-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 97 AND PULocationID < 147 AND
      DOLocationID > 8 AND DOLocationID < 61
