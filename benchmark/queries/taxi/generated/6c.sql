-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 85.0045313808268 AND PULocationID < 130.34775784730783 AND
      DOLocationID > 65.54676155853369 AND DOLocationID < 164.3596237854994
AND tpep_pickup_datetime > '2018071919:48:19' AND
    tpep_pickup_datetime < '2019060405:18:52' AND
    tpep_dropoff_datetime > '2018031608:08:52' AND
    tpep_dropoff_datetime < '2019103017:25:41'
AND
    passenger_count > 3.033766813460036 AND passenger_count < 4.538805404662212 AND
    fare_amount > 15.974024562889156 AND fare_amount < 342.43077715894657
