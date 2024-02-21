-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 129 AND PULocationID < 194 AND
      DOLocationID > 46 AND DOLocationID < 80
