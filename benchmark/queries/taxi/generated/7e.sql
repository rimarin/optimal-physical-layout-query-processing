-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where DOLocationID > 59.29474004081598 AND DOLocationID < 170.14505222018298
AND tpep_pickup_datetime > '2018010409:28:44' AND
    tpep_pickup_datetime < '2019122815:21:28' AND
    tpep_dropoff_datetime > '2018110915:18:20' AND
    tpep_dropoff_datetime < '2019013021:17:03'
AND
    passenger_count > 1.668436862348133 AND passenger_count < 6.2561329056308566 AND
    fare_amount > 30.069178531282994 AND fare_amount < 266.258560571143 AND
    trip_distance > 1.4376200705274864 AND trip_distance < 10.620486770549574
