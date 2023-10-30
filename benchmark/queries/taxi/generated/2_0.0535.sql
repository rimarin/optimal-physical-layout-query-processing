-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 164.13451680060183 AND PULocationID < 259.52369054598023 AND
      DOLocationID > 192.1668716670904 AND DOLocationID < 194.55333992983168
