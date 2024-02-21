-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 32 AND PULocationID < 65 AND
    DOLocationID > 117 AND DOLocationID < 206
AND tpep_pickup_datetime > '2018022613:50:52' AND
    tpep_pickup_datetime < '2019052801:55:15'
