-- Q3
SELECT *
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > ':1' AND PULocationID < ':2' AND
    DOLocationID > ':3' AND DOLocationID < ':4'
AND tpep_pickup_datetime > ':5' AND
    tpep_pickup_datetime < ':6'
