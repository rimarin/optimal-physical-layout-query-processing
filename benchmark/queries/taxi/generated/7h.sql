-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/trips*.parquet') where DOLocationID > 59.29474004081598 AND DOLocationID < 170.14505222018298
AND tpep_pickup_datetime > '2018010409:28:44' AND
    tpep_pickup_datetime < '2019122815:21:28' AND
    tpep_dropoff_datetime > '2018110915:18:20' AND
    tpep_dropoff_datetime < '2019013021:17:03'
AND
    passenger_count > 1.668436862348133 AND passenger_count < 6.2561329056308566 AND
    fare_amount > 8.440960169240174 AND fare_amount < 274.148269288919 AND
    trip_distance > 1.259324355822188 AND trip_distance < 3.936273294674817
