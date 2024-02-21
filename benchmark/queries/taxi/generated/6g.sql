-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 38 AND PULocationID < 178 AND
      DOLocationID > 154 AND DOLocationID < 242
AND tpep_pickup_datetime > '2018051921:35:02' AND
    tpep_pickup_datetime < '2019032012:43:03' AND
    tpep_dropoff_datetime > '2018052720:31:37' AND
    tpep_dropoff_datetime < '2019082917:36:41'
AND
    passenger_count > 1 AND passenger_count < 7 AND
    fare_amount > 29.956722989991412 AND fare_amount < 64.57067313309727
