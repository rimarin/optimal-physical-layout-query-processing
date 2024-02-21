-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 88 AND PULocationID < 165 AND
      DOLocationID > 162 AND DOLocationID < 166
