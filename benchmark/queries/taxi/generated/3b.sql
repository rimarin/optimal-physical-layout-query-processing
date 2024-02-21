-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 28.885437052582372 AND PULocationID < 39.48484720112557 AND
    DOLocationID > 77.6690758234806 AND DOLocationID < 161.0912082058815
AND tpep_pickup_datetime > '2018110720:07:06' AND
    tpep_pickup_datetime < '2019110807:07:21'
