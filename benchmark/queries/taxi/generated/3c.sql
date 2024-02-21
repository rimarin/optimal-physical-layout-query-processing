-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 168 AND PULocationID < 243 AND
    DOLocationID > 205 AND DOLocationID < 224
AND tpep_pickup_datetime > '2018071509:56:52' AND
    tpep_pickup_datetime < '2019080707:13:59'
