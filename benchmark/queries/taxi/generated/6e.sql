-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 107 AND PULocationID < 143 AND
      DOLocationID > 15 AND DOLocationID < 243
AND tpep_pickup_datetime > '2018080616:39:18' AND
    tpep_pickup_datetime < '2019062319:36:03' AND
    tpep_dropoff_datetime > '2018052517:50:37' AND
    tpep_dropoff_datetime < '2019110904:58:41'
AND
    passenger_count > 3 AND passenger_count < 5 AND
    fare_amount > 33.07497929605419 AND fare_amount < 448.86021914062854
