-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 32.33689592133864 AND PULocationID < 64.84793261274407 AND
    DOLocationID > 117.90397274362047 AND DOLocationID < 205.1565801937608
AND tpep_pickup_datetime > '2018022613:50:52' AND
    tpep_pickup_datetime < '2019052801:55:15'
