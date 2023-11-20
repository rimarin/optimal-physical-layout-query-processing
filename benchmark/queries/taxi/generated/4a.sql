-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 16.9861675310669 AND PULocationID < 38.47494800857123 AND
      DOLocationID > 81.54677257101044 AND DOLocationID < 200.503925572706
AND tpep_pickup_datetime > '2018033002:54:17' AND
    tpep_pickup_datetime < '2019071616:08:34' AND
    tpep_dropoff_datetime > '2018041900:43:35' AND
    tpep_dropoff_datetime < '2019060113:57:08'
