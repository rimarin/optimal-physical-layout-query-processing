-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 43.05044991865496 AND PULocationID < 93.90380413103983 AND
    DOLocationID > 25.092910551639093 AND DOLocationID < 162.128528746554
AND tpep_pickup_datetime > '2018081416:57:02' AND
    tpep_pickup_datetime < '2019111001:27:22'
