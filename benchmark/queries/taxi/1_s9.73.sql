SELECT *
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > 13 AND PULocationID < 70
