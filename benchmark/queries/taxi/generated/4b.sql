-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 119.84650914730183 AND PULocationID < 125.3297234763639 AND
      DOLocationID > 2.662555454147939 AND DOLocationID < 153.30337699520413
AND tpep_pickup_datetime > '2018082112:01:21' AND
    tpep_pickup_datetime < '2019091702:50:15' AND
    tpep_dropoff_datetime > '2018061312:46:10' AND
    tpep_dropoff_datetime < '2019061909:49:53'
