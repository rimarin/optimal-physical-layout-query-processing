-- Q5
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -169.682538825102 AND min_lon < 47.09579390439657 AND
      max_lon > -74.50456011733336 AND max_lon < 59.532334169883995 AND
      min_lat > -16.257652651069137 AND min_lat < 54.785878160665135 AND
      max_lat > 53.042530199271084 AND max_lat < 61.091506727967555 AND
      created_at > '2007101205:17:12' AND created_at < '2021040619:55:37'