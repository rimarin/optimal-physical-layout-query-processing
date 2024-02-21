-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 134.72587616154993 AND PULocationID < 192.93097078501623 AND
      DOLocationID > 217.09383394665994 AND DOLocationID < 260.12126146770106
AND tpep_pickup_datetime > '2018011605:19:04' AND
    tpep_pickup_datetime < '2019060219:03:54' AND
    tpep_dropoff_datetime > '2018081705:47:46' AND
    tpep_dropoff_datetime < '2019012218:30:57'
AND
    passenger_count > 3.0144742589569935 AND passenger_count < 7.680468919469273 AND
    fare_amount > 27.254947277472464 AND fare_amount < 198.3205806525993
