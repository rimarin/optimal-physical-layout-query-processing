-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 168.59950883178067 AND PULocationID < 242.68104372278154 AND
    DOLocationID > 205.9261543305353 AND DOLocationID < 223.37129739374777
AND tpep_pickup_datetime > '2018071509:56:52' AND
    tpep_pickup_datetime < '2019080707:13:59'
