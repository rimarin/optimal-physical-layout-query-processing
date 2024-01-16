-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -135.48180851777872 AND min_lon < 18.806871083768698 AND
      max_lon > -59.90732800534059 AND max_lon < -58.72402760171937 AND
      min_lat > -84.41742466512552 AND min_lat < 17.53682092103466