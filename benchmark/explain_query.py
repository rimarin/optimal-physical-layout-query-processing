import duckdb

duckdb.sql("EXPLAIN ANALYZE SELECT * FROM read_parquet('../test/NoPartition/*.parquet') WHERE month < 6 and day > 15")
duckdb.sql("EXPLAIN ANALYZE SELECT * FROM read_parquet('../test/FixedGrid/*.parquet') WHERE month < 6 and day > 15")
