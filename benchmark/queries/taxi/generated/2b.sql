-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 23 AND PULocationID < 167 AND
      DOLocationID > 101 AND DOLocationID < 106
