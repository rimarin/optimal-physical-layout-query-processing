-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 1 AND PULocationID < 61 AND
      DOLocationID > 86 AND DOLocationID < 262
AND tpep_pickup_datetime > '2018030304:03:16' AND
    tpep_pickup_datetime < '2019011320:52:55' AND
    tpep_dropoff_datetime > '2018111102:02:25' AND
    tpep_dropoff_datetime < '2019072906:47:57'
AND
    passenger_count > 1 AND passenger_count < 5 AND
    fare_amount > 54.278061192494064 AND fare_amount < 372.39803957946856
