-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 28 AND PULocationID < 40 AND
    DOLocationID > 77 AND DOLocationID < 162
AND tpep_pickup_datetime > '2018110720:07:06' AND
    tpep_pickup_datetime < '2019110807:07:21'
