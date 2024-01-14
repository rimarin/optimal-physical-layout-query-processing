-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -57.11222447213618 AND min_lon < 123.22657626427025 AND
      max_lon > 73.86214474557232 AND max_lon < 82.43925503825335 AND
      min_lat > -33.46794515603564 AND min_lat < 43.300572805073415 AND
      max_lat > 1.0897769961240868 AND max_lat < 56.402414171799904