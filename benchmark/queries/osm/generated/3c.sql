-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -149.49083865875744 AND min_lon < -18.50554698439163 AND
      max_lon > -165.9379548751926 AND max_lon < -96.44876239481312 AND
      min_lat > -63.97753348003427 AND min_lat < -0.21593762140462047