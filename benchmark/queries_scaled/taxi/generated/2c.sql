-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 67 AND PULocationID < 93 AND
      DOLocationID > 33 AND DOLocationID < 43
