-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 175 AND PULocationID < 182 AND
      DOLocationID > 76 AND DOLocationID < 82
