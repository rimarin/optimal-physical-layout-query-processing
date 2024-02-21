-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 143 AND PULocationID < 205 AND
      DOLocationID > 26 AND DOLocationID < 74
AND tpep_pickup_datetime > '2018111205:36:49' AND
    tpep_pickup_datetime < '2019101411:11:12' AND
    tpep_dropoff_datetime > '2018072423:53:46' AND
    tpep_dropoff_datetime < '2019010907:43:49'
