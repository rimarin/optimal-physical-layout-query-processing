-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 59.29564685122759 AND PULocationID < 227.73011873227443 AND
      DOLocationID > 154.88493382864584 AND DOLocationID < 242.14159475970317
AND tpep_pickup_datetime > '2018050109:36:13' AND
    tpep_pickup_datetime < '2019100110:48:08' AND
    tpep_dropoff_datetime > '2018081103:06:08' AND
    tpep_dropoff_datetime < '2019111421:41:44'
AND
    passenger_count > 1.47577538998768 AND passenger_count < 5.288029654098254 AND
    fare_amount > 17.63245545030301 AND fare_amount < 102.0915471416241 AND
    trip_distance > 4.013138517451512 AND trip_distance < 21.61329074916304
