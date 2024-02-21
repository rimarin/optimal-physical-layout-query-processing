-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 109 AND PULocationID < 243 AND
      DOLocationID > 28 AND DOLocationID < 216
AND tpep_pickup_datetime > '2018030717:51:44' AND
    tpep_pickup_datetime < '2019011310:03:24' AND
    tpep_dropoff_datetime > '2018092215:47:29' AND
    tpep_dropoff_datetime < '2019110808:28:27'
AND
    passenger_count > 3 AND passenger_count < 6
