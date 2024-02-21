-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 119 AND PULocationID < 126 AND
      DOLocationID > 2 AND DOLocationID < 154
AND tpep_pickup_datetime > '2018082112:01:21' AND
    tpep_pickup_datetime < '2019091702:50:15' AND
    tpep_dropoff_datetime > '2018061312:46:10' AND
    tpep_dropoff_datetime < '2019061909:49:53'
