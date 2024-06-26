-- Q6
SELECT *
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > ':1' AND PULocationID < ':2' AND
      DOLocationID > ':3' AND DOLocationID < ':4' AND
      tpep_pickup_datetime > ':5' AND tpep_pickup_datetime < ':6' AND
      tpep_dropoff_datetime > ':7' AND tpep_dropoff_datetime < ':8' AND
      passenger_count > ':9' AND passenger_count < ':10' AND
      fare_amount > ':11' AND fare_amount < ':12'
