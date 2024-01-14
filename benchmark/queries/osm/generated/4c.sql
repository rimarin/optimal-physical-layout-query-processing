-- Q4
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -80.59897733812171 AND min_lon < 166.52443529381668 AND
      max_lon > 9.174821000020557 AND max_lon < 60.6832337787591 AND
      min_lat > -36.48923084750457 AND min_lat < -28.44587500286628 AND
      max_lat > -32.23956410742203 AND max_lat < 73.38792219805904