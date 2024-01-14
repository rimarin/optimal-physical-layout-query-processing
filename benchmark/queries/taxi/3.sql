-- Q3
SELECT *
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > 129.52788054320652 AND PULocationID < 193.36995644581523 AND
      DOLocationID > 46.2786959342697 AND DOLocationID < 79.89332387836889 AND
      tpep_pickup_datetime > ':5' AND tpep_pickup_datetime < ':6'
