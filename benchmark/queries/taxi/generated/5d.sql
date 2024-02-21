-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where PULocationID > 219.65722958543356 AND PULocationID < 262.9569408080622 AND
      DOLocationID > 102.37667026530139 AND DOLocationID < 137.98407535200806
AND tpep_pickup_datetime > '2018091420:14:22' AND
    tpep_pickup_datetime < '2019041316:31:11' AND
    tpep_dropoff_datetime > '2018070407:41:59' AND
    tpep_dropoff_datetime < '2019071114:22:26'
AND
    passenger_count > 1.87293532660905 AND passenger_count < 6.616039437463073
