-- Q7
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/taxi/*.parquet') where DOLocationID > 59 AND DOLocationID < 171
AND tpep_pickup_datetime > '2018010409:28:44' AND
    tpep_pickup_datetime < '2019122815:21:28' AND
    tpep_dropoff_datetime > '2018110915:18:20' AND
    tpep_dropoff_datetime < '2019013021:17:03'
AND
    passenger_count > 1 AND passenger_count < 7 AND
    fare_amount > 23.45324107662189 AND fare_amount < 169.6689191764722 AND
    trip_distance > 24.926307759746837 AND trip_distance < 35.7972574490449
