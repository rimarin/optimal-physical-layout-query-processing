select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM read_parquet('/home/brancaleone/Projects/optimal-physical-layout-query-processing/benchmark/datasets/tpch/lineitem*.parquet') where
	l_partkey = p_partkey
	and l_shipdate >= date '1996-12-01'
	and l_shipdate < date '1996-12-01' + interval '1' month;
