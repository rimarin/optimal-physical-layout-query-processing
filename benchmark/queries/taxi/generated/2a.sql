-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 159 AND PULocationID < 242 AND
      DOLocationID > 46 AND DOLocationID < 48
