-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 85 AND PULocationID < 131 AND
      DOLocationID > 65 AND DOLocationID < 165
AND tpep_pickup_datetime > '2018071919:48:19' AND
    tpep_pickup_datetime < '2019060405:18:52' AND
    tpep_dropoff_datetime > '2018031608:08:52' AND
    tpep_dropoff_datetime < '2019103017:25:41'
AND
    passenger_count > 3 AND passenger_count < 5 AND
    fare_amount > 15.974024562889156 AND fare_amount < 342.43077715894657
