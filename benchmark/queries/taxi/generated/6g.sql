-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 38.378252366071145 AND PULocationID < 177.31976783241115 AND
      DOLocationID > 154.10500447593313 AND DOLocationID < 241.05713222117473
AND tpep_pickup_datetime > '2018051921:35:02' AND
    tpep_pickup_datetime < '2019032012:43:03' AND
    tpep_dropoff_datetime > '2018052720:31:37' AND
    tpep_dropoff_datetime < '2019082917:36:41'
AND
    passenger_count > 1.699649400596165 AND passenger_count < 6.276049095552217 AND
    fare_amount > 29.956722989991412 AND fare_amount < 64.57067313309727
