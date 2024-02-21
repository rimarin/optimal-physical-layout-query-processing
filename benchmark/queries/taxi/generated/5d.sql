-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 219 AND PULocationID < 263 AND
      DOLocationID > 102 AND DOLocationID < 138
AND tpep_pickup_datetime > '2018091420:14:22' AND
    tpep_pickup_datetime < '2019041316:31:11' AND
    tpep_dropoff_datetime > '2018070407:41:59' AND
    tpep_dropoff_datetime < '2019071114:22:26'
AND
    passenger_count > 1 AND passenger_count < 7
