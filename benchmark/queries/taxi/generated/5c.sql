-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 241 AND PULocationID < 252 AND
      DOLocationID > 111 AND DOLocationID < 180
AND tpep_pickup_datetime > '2018042201:00:35' AND
    tpep_pickup_datetime < '2019111014:38:20' AND
    tpep_dropoff_datetime > '2018070519:47:45' AND
    tpep_dropoff_datetime < '2019122800:43:54'
AND
    passenger_count > 2 AND passenger_count < 6
