-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 65 AND PULocationID < 102 AND
    DOLocationID > 59 AND DOLocationID < 262
AND tpep_pickup_datetime > '2018120103:27:16' AND
    tpep_pickup_datetime < '2019030706:45:48'
