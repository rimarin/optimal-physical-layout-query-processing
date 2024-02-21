-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 59 AND PULocationID < 228 AND
      DOLocationID > 154 AND DOLocationID < 243
AND tpep_pickup_datetime > '2018050109:36:13' AND
    tpep_pickup_datetime < '2019100110:48:08' AND
    tpep_dropoff_datetime > '2018081103:06:08' AND
    tpep_dropoff_datetime < '2019111421:41:44'
AND
    passenger_count > 1 AND passenger_count < 6 AND
    fare_amount > 17.63245545030301 AND fare_amount < 102.0915471416241 AND
    trip_distance > 4.013138517451512 AND trip_distance < 21.61329074916304
