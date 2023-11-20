-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 6.82961897304517 AND PULocationID < 33.893321809388254 AND
      DOLocationID > 168.14818845957225 AND DOLocationID < 222.80703128590088
AND tpep_pickup_datetime > '2018080518:59:23' AND
    tpep_pickup_datetime < '2019120717:41:15' AND
    tpep_dropoff_datetime > '2018122103:05:18' AND
    tpep_dropoff_datetime < '2019102817:00:20'
AND
    passenger_count > 1.1328018795961121 AND passenger_count < 2.456057548401441
