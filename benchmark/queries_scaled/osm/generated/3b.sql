-- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -145.29665268468023 AND min_lon < -136.64776393954995 AND
      max_lon > -166.37375755142335 AND max_lon < 95.76765760661857 AND
      min_lat > -8.373579453574564 AND min_lat < 87.63563728048538