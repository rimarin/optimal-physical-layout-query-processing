-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 59.371342603034506 AND PULocationID < 252.7271042854256 AND
      DOLocationID > 66.51549871723638 AND DOLocationID < 182.93957790346096
AND tpep_pickup_datetime > '2018123111:13:14' AND
    tpep_pickup_datetime < '2019122804:24:50' AND
    tpep_dropoff_datetime > '2018020119:47:31' AND
    tpep_dropoff_datetime < '2019110211:59:44'
AND
    passenger_count > 3.031501885049428 AND passenger_count < 6.0054160703711945 AND
    fare_amount > 48.49910816097919 AND fare_amount < 191.70787975704224
