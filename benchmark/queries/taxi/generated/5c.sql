-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 241.1154248495449 AND PULocationID < 251.48522941952598 AND
      DOLocationID > 111.83789538773615 AND DOLocationID < 179.9897826274831
AND tpep_pickup_datetime > '2018042201:00:35' AND
    tpep_pickup_datetime < '2019111014:38:20' AND
    tpep_dropoff_datetime > '2018070519:47:45' AND
    tpep_dropoff_datetime < '2019122800:43:54'
AND
    passenger_count > 2.4025145230259572 AND passenger_count < 5.651062538533387
