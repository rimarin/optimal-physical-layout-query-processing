-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 165 AND PULocationID < 240 AND
      DOLocationID > 25 AND DOLocationID < 154
AND tpep_pickup_datetime > '2018072723:53:45' AND
    tpep_pickup_datetime < '2019062320:43:06' AND
    tpep_dropoff_datetime > '2018121621:39:15' AND
    tpep_dropoff_datetime < '2019022415:02:27'
AND
    passenger_count > 1 AND passenger_count < 3
