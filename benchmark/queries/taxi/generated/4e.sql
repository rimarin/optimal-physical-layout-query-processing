-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 143.6715457672024 AND PULocationID < 204.69834053635296 AND
      DOLocationID > 26.590092909913345 AND DOLocationID < 74.03601963570438
AND tpep_pickup_datetime > '2018111205:36:49' AND
    tpep_pickup_datetime < '2019101411:11:12' AND
    tpep_dropoff_datetime > '2018072423:53:46' AND
    tpep_dropoff_datetime < '2019010907:43:49'
