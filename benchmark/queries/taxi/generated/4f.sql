-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 138 AND PULocationID < 257 AND
      DOLocationID > 68 AND DOLocationID < 101
AND tpep_pickup_datetime > '2018101711:30:18' AND
    tpep_pickup_datetime < '2019090409:40:18' AND
    tpep_dropoff_datetime > '2018030712:25:20' AND
    tpep_dropoff_datetime < '2019032711:17:08'
