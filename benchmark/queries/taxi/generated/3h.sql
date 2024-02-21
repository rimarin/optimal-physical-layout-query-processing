-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 65.32830340691778 AND PULocationID < 101.30681592278495 AND
    DOLocationID > 59.89037368842183 AND DOLocationID < 261.09041895290414
AND tpep_pickup_datetime > '2018120103:27:16' AND
    tpep_pickup_datetime < '2019030706:45:48'
