-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -166.30864146118563 AND min_lon < -75.49794461110945 AND
      max_lon > -165.2723990368443 AND max_lon < 131.92885202710414 AND
      min_lat > -20.305024625622707 AND min_lat < 35.907688935583906 AND
      max_lat > -28.454119165386658 AND max_lat < 12.855336478451392 AND
      created_at > '2009010120:13:28' AND created_at < '2016031215:19:09'