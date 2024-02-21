-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 134 AND PULocationID < 193 AND
      DOLocationID > 217 AND DOLocationID < 261
AND tpep_pickup_datetime > '2018011605:19:04' AND
    tpep_pickup_datetime < '2019060219:03:54' AND
    tpep_dropoff_datetime > '2018081705:47:46' AND
    tpep_dropoff_datetime < '2019012218:30:57'
AND
    passenger_count > 3 AND passenger_count < 8 AND
    fare_amount > 27.254947277472464 AND fare_amount < 198.3205806525993
