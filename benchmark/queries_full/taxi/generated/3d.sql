-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 45.38130005414181 AND PULocationID < 60.292692434001125 AND
    DOLocationID > 2.5303418216367337 AND DOLocationID < 117.8948138057379
AND tpep_pickup_datetime > '2018030307:00:28' AND
    tpep_pickup_datetime < '2019050409:25:19'
