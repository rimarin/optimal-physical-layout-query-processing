-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 219 AND PULocationID < 253 AND
      DOLocationID > 44 AND DOLocationID < 58
AND tpep_pickup_datetime > '2018100803:09:24' AND
    tpep_pickup_datetime < '2019012404:25:46' AND
    tpep_dropoff_datetime > '2018071520:53:14' AND
    tpep_dropoff_datetime < '2019121222:27:43'
