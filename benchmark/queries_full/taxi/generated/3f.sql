-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 65.13345124283873 AND PULocationID < 140.72076561552822 AND
    DOLocationID > 95.45623433530403 AND DOLocationID < 149.81733233001103
AND tpep_pickup_datetime > '2018020114:55:17' AND
    tpep_pickup_datetime < '2019080211:17:23'
