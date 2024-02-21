-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 78 AND PULocationID < 116 AND
      DOLocationID > 76 AND DOLocationID < 115
AND tpep_pickup_datetime > '2018030221:15:07' AND
    tpep_pickup_datetime < '2019042817:58:00' AND
    tpep_dropoff_datetime > '2018041700:13:54' AND
    tpep_dropoff_datetime < '2019082521:43:18'
AND
    passenger_count > 3 AND passenger_count < 8 AND
    fare_amount > 25.466704765756244 AND fare_amount < 256.4898695096622
