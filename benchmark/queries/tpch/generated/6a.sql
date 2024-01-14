-- using 1705221898 as a seed to the RNG


select
	sum(l_extendedprice * l_discount) as revenue
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/tpch-sf10/no-partition/tpch-sf10*.parquet') where
	l_shipdate >= date '1996-01-01'
	and l_shipdate < date '1996-01-01' + interval '1' year
	and l_discount between 0.09 - 0.01 and 0.09 + 0.01
	and l_quantity < 24;
