-- Q2
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > 33.60896587756085 AND min_lon < 34.65676979022865 AND
      max_lon > 22.458789902230706 AND max_lon < 164.58561998854822