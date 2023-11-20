-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 109.8952676500609 AND PULocationID < 242.10983635720288 AND
      DOLocationID > 28.823916225981943 AND DOLocationID < 215.11193178718975
AND tpep_pickup_datetime > '2018030717:51:44' AND
    tpep_pickup_datetime < '2019011310:03:24' AND
    tpep_dropoff_datetime > '2018092215:47:29' AND
    tpep_dropoff_datetime < '2019110808:28:27'
AND
    passenger_count > 3.807018791072471 AND passenger_count < 5.737550708378743
