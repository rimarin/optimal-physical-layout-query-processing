-- Q6
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 78.11303334416921 AND PULocationID < 115.26419632195635 AND
      DOLocationID > 76.6634519260828 AND DOLocationID < 114.53847737859745
AND tpep_pickup_datetime > '2018030221:15:07' AND
    tpep_pickup_datetime < '2019042817:58:00' AND
    tpep_dropoff_datetime > '2018041700:13:54' AND
    tpep_dropoff_datetime < '2019082521:43:18'
AND
    passenger_count > 3.1715069906463134 AND passenger_count < 7.115285194419929 AND
    fare_amount > 25.466704765756244 AND fare_amount < 256.4898695096622
