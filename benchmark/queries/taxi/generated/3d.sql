-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 45 AND PULocationID < 61 AND
    DOLocationID > 2 AND DOLocationID < 118
AND tpep_pickup_datetime > '2018030307:00:28' AND
    tpep_pickup_datetime < '2019050409:25:19'
