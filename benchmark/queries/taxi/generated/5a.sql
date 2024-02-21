-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 8 AND PULocationID < 18 AND
      DOLocationID > 182 AND DOLocationID < 230
AND tpep_pickup_datetime > '2018052215:49:13' AND
    tpep_pickup_datetime < '2019091900:16:35' AND
    tpep_dropoff_datetime > '2018090117:01:42' AND
    tpep_dropoff_datetime < '2019013017:42:54'
AND
    passenger_count > 1 AND passenger_count < 4
