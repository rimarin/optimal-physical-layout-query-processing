-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 30 AND PULocationID < 136 AND
      DOLocationID > 43 AND DOLocationID < 82
