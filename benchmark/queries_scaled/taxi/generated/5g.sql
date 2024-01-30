-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where PULocationID > 107.16079410965308 AND PULocationID < 260.365382647284 AND
      DOLocationID > 51.06818301822582 AND DOLocationID < 214.78246578418893
AND tpep_pickup_datetime > '2018012717:28:42' AND
    tpep_pickup_datetime < '2019100613:03:53' AND
    tpep_dropoff_datetime > '2018022200:34:31' AND
    tpep_dropoff_datetime < '2019060800:29:28'
AND
    passenger_count > 1.707833529989753 AND passenger_count < 2.2745772037318046
