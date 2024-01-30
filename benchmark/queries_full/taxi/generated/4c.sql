-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 123.7392698678591 AND PULocationID < 129.51857150738795 AND
      DOLocationID > 85.93506103637691 AND DOLocationID < 229.76926623410301
AND tpep_pickup_datetime > '2018073104:13:34' AND
    tpep_pickup_datetime < '2019040809:11:36' AND
    tpep_dropoff_datetime > '2018080111:48:43' AND
    tpep_dropoff_datetime < '2019062408:18:16'
