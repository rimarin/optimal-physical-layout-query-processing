-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 37 AND PULocationID < 258 AND
      DOLocationID > 147 AND DOLocationID < 187
AND tpep_pickup_datetime > '2018032914:40:43' AND
    tpep_pickup_datetime < '2019090321:45:52' AND
    tpep_dropoff_datetime > '2018071309:17:33' AND
    tpep_dropoff_datetime < '2019052323:23:20'
AND
    passenger_count > 1 AND passenger_count < 4 AND
    fare_amount > 8.510967760635147 AND fare_amount < 370.1165562417815
