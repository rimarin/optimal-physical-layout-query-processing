-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 187.11198067918795 AND PULocationID < 207.54646233380475 AND
    DOLocationID > 98.4226693066787 AND DOLocationID < 130.16699672261458
AND tpep_pickup_datetime > '2018031617:43:45' AND
    tpep_pickup_datetime < '2019100506:11:49'
