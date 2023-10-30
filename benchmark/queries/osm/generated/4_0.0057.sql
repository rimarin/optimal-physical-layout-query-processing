-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/*.parquet') where min_lon > -8.141789583807224 AND min_lon < 22.67790556690167 AND
      max_lon > -0.04459453245510758 AND max_lon < 88.93643234772475 AND
      min_lat > 15.316373308392443 AND min_lat < 66.3183000444715 AND
      max_lat > 11.660666334866292 AND max_lat < 16.789535238360372