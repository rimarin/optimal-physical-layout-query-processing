-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 187 AND PULocationID < 208 AND
    DOLocationID > 98 AND DOLocationID < 131
AND tpep_pickup_datetime > '2018031617:43:45' AND
    tpep_pickup_datetime < '2019100506:11:49'
