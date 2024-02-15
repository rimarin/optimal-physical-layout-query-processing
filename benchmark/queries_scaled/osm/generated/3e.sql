- Q3
SELECT *
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/osm/no-partition/*.parquet') where min_lon > -22.225830190704187 AND min_lon < -13.015879706825388 AND
      max_lon > -155.1064908161818 AND max_lon < 178.64594732619423 AND
      min_lat > 15.425900928652638 AND min_lat < 34.30883168623366