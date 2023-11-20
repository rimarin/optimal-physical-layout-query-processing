-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 165.5463323944325 AND PULocationID < 239.4520138708776 AND
      DOLocationID > 25.453824732364044 AND DOLocationID < 153.521834542937
AND tpep_pickup_datetime > '2018072723:53:45' AND
    tpep_pickup_datetime < '2019062320:43:06' AND
    tpep_dropoff_datetime > '2018121621:39:15' AND
    tpep_dropoff_datetime < '2019022415:02:27'
AND
    passenger_count > 1.4230321990899237 AND passenger_count < 2.4862260396761706
