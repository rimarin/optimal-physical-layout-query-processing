-- Q4
SELECT COUNT(*)
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > ':1' AND PULocationID < ':2'
      DOLocationID > ':3' AND DOLocationID < ':4'
AND tpep_pickup_datetime > DATE ':5' AND
    tpep_pickup_datetime < DATE ':6' AND
    tpep_dropoff_datetime > DATE ':7' AND
    tpep_dropoff_datetime < DATE ':8'
