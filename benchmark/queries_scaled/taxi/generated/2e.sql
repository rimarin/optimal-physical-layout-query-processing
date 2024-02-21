-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 121 AND PULocationID < 150 AND
      DOLocationID > 184 AND DOLocationID < 224
