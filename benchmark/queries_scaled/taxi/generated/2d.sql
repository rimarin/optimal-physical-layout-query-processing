-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 31.783583145510637 AND PULocationID < 107.0274842460507 AND
      DOLocationID > 26.57122949938106 AND DOLocationID < 42.044659818879744
