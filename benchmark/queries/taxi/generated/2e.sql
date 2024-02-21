-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 42 AND PULocationID < 69 AND
      DOLocationID > 19 AND DOLocationID < 51
