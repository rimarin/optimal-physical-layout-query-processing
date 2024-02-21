-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 162 AND PULocationID < 251 AND
      DOLocationID > 96 AND DOLocationID < 130
