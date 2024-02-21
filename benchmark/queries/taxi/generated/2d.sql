-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 9 AND PULocationID < 168 AND
      DOLocationID > 95 AND DOLocationID < 100
