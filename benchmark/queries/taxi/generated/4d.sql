-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 219.84571747146998 AND PULocationID < 252.59643269830698 AND
      DOLocationID > 44.004605610069774 AND DOLocationID < 57.435362380698585
AND tpep_pickup_datetime > '2018100803:09:24' AND
    tpep_pickup_datetime < '2019012404:25:46' AND
    tpep_dropoff_datetime > '2018071520:53:14' AND
    tpep_dropoff_datetime < '2019121222:27:43'
