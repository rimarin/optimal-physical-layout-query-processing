-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 164 AND PULocationID < 260 AND
      DOLocationID > 192 AND DOLocationID < 195
