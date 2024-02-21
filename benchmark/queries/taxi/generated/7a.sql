-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 15 AND PULocationID < 116 AND
      DOLocationID > 184 AND DOLocationID < 199
AND tpep_pickup_datetime > '2018042620:45:55' AND
    tpep_pickup_datetime < '2019091913:29:36' AND
    tpep_dropoff_datetime > '2018083012:46:53' AND
    tpep_dropoff_datetime < '2019070900:55:48'
AND
    passenger_count > 4 AND passenger_count < 7 AND
    fare_amount > 24.391407777882513 AND fare_amount < 246.92757322174032 AND
    trip_distance > 6.47878008195973 AND trip_distance < 16.463434556402774
