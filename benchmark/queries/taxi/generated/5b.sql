-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 6 AND PULocationID < 34 AND
      DOLocationID > 168 AND DOLocationID < 223
AND tpep_pickup_datetime > '2018080518:59:23' AND
    tpep_pickup_datetime < '2019120717:41:15' AND
    tpep_dropoff_datetime > '2018122103:05:18' AND
    tpep_dropoff_datetime < '2019102817:00:20'
AND
    passenger_count > 1 AND passenger_count < 3
