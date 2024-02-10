-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 107.6221406466069 AND PULocationID < 142.65396699546605 AND
      DOLocationID > 15.46052574525671 AND DOLocationID < 242.21649004395107
AND tpep_pickup_datetime > '2018080616:39:18' AND
    tpep_pickup_datetime < '2019062319:36:03' AND
    tpep_dropoff_datetime > '2018052517:50:37' AND
    tpep_dropoff_datetime < '2019110904:58:41'
AND
    passenger_count > 3.0761749603517035 AND passenger_count < 4.57073638144017 AND
    fare_amount > 33.07497929605419 AND fare_amount < 448.86021914062854
