SELECT *
FROM read_parquet('datasets/taxi/*.parquet')
WHERE PULocationID > 120 AND PULocationID < 130
