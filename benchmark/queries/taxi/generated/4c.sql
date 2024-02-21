-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 123 AND PULocationID < 130 AND
      DOLocationID > 85 AND DOLocationID < 230
AND tpep_pickup_datetime > '2018073104:13:34' AND
    tpep_pickup_datetime < '2019040809:11:36' AND
    tpep_dropoff_datetime > '2018080111:48:43' AND
    tpep_dropoff_datetime < '2019062408:18:16'
