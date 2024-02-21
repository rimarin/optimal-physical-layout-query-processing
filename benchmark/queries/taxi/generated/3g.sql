-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 43 AND PULocationID < 94 AND
    DOLocationID > 25 AND DOLocationID < 163
AND tpep_pickup_datetime > '2018081416:57:02' AND
    tpep_pickup_datetime < '2019111001:27:22'
