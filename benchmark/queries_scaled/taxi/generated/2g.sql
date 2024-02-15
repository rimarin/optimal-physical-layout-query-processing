-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/no-partition/*.parquet') where PULocationID > 97.31571947829384 AND PULocationID < 146.7124099864747 AND
      DOLocationID > 8.954815573670631 AND DOLocationID < 60.683228297192045
