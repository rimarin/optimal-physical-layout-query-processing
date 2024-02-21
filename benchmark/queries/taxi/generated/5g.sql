-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 107 AND PULocationID < 261 AND
      DOLocationID > 51 AND DOLocationID < 215
AND tpep_pickup_datetime > '2018012717:28:42' AND
    tpep_pickup_datetime < '2019100613:03:53' AND
    tpep_dropoff_datetime > '2018022200:34:31' AND
    tpep_dropoff_datetime < '2019060800:29:28'
AND
    passenger_count > 1 AND passenger_count < 3
