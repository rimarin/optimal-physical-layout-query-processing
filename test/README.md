The unit tests have been designed to run fast and therefore check only small toy datasets.
Unit tests involving the full-scale dataset are already present, but deactivated.
Assertions are made on the number of rows and partitions.
In the future it is reasonable to extend these correctness checks with queries over 
the dataset, looking for the presence of specific values, or absences of duplicates.

Manual testing tips:

- Launch the partitioner
```
../cmake-build-release/partitioner/partitioner benchmark/datasets/taxi taxi hilbert-curve 250000 PULocationID,DOLocationID
```

- Run an example query on the partitioned dataset

```
:memory:
"SELECT * FROM read_parquet('benchmark/datasets/taxi/hilbert-curve/*.parquet') where PULocationID > 84 AND PULocationID < 86"
```

- Obtain a summary of the files, the row groups, and their statistics 

```
CREATE TABLE summary AS
SELECT file_name, path_in_schema, row_group_id, row_group_num_rows, stats_min, stats_max
FROM parquet_metadata('benchmark/datasets/taxi/hilbert-curve/*.parquet')
WHERE path_in_schema='PULocationID
GROUP BY file_name, path_in_schema, row_group_id, row_group_num_rows, stats_min, stats_max
ORDER BY file_name, row_group_id, path_in_schema;
```

- You should be able to see the fetched row groups with:

```
SELECT file_name, row_group_id, stats_min, stats_max 
FROM summary 
WHERE stats_min > 84 AND stats_max < 86 
GROUP BY file_name, row_group_id, stats_min, stats_max 
ORDER BY file_name;
```
