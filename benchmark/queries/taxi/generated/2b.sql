-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 23.386542175171094 AND PULocationID < 166.3731418298763 AND
      DOLocationID > 101.58785570583501 AND DOLocationID < 105.8529543169786
