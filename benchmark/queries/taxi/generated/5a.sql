-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 8.064428117765225 AND PULocationID < 17.140219089705525 AND
      DOLocationID > 182.476552403154 AND DOLocationID < 229.28121474654446
AND tpep_pickup_datetime > '2018052215:49:13' AND
    tpep_pickup_datetime < '2019091900:16:35' AND
    tpep_dropoff_datetime > '2018090117:01:42' AND
    tpep_dropoff_datetime < '2019013017:42:54'
AND
    passenger_count > 1.093248026245269 AND passenger_count < 3.411129594203601
