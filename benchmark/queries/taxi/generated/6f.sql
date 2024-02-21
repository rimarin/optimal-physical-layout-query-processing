-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 59 AND PULocationID < 253 AND
      DOLocationID > 66 AND DOLocationID < 183
AND tpep_pickup_datetime > '2018123111:13:14' AND
    tpep_pickup_datetime < '2019122804:24:50' AND
    tpep_dropoff_datetime > '2018020119:47:31' AND
    tpep_dropoff_datetime < '2019110211:59:44'
AND
    passenger_count > 3 AND passenger_count < 7 AND
    fare_amount > 48.49910816097919 AND fare_amount < 191.70787975704224
