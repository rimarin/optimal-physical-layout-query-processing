-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 74 AND PULocationID < 216 AND
      DOLocationID > 28 AND DOLocationID < 87
AND tpep_pickup_datetime > '2018101512:15:53' AND
    tpep_pickup_datetime < '2019120702:30:59' AND
    tpep_dropoff_datetime > '2018050621:02:22' AND
    tpep_dropoff_datetime < '2019061501:25:32'
