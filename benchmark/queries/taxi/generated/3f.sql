-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 65 AND PULocationID < 141 AND
    DOLocationID > 95 AND DOLocationID < 150
AND tpep_pickup_datetime > '2018020114:55:17' AND
    tpep_pickup_datetime < '2019080211:17:23'
