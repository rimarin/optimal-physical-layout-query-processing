-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 16 AND PULocationID < 39 AND
      DOLocationID > 81 AND DOLocationID < 201
AND tpep_pickup_datetime > '2018033002:54:17' AND
    tpep_pickup_datetime < '2019071616:08:34' AND
    tpep_dropoff_datetime > '2018041900:43:35' AND
    tpep_dropoff_datetime < '2019060113:57:08'
