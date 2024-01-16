-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -109.49770963224455 AND min_lon < 47.49631048536182 AND
      max_lon > -95.22191927021527 AND max_lon < 177.16532417073262 AND
      min_lat > -66.81824111268578 AND min_lat < -58.29520313572964