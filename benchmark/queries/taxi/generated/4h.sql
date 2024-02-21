-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 98 AND PULocationID < 162 AND
      DOLocationID > 196 AND DOLocationID < 265
AND tpep_pickup_datetime > '2018061712:34:21' AND
    tpep_pickup_datetime < '2019050200:43:10' AND
    tpep_dropoff_datetime > '2018110210:54:00' AND
    tpep_dropoff_datetime < '2019051213:29:38'
