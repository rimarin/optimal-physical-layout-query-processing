# Towards an Optimal Physical Layout for Efficient Query Processing in the Cloud

With the advent of cloud-based data warehouses, a new paradigm that separates compute and storage has been established. As a consequence, the use of indexing has not been made its way in many systems, in favor of the use of partitioning. At the same time, gradually more techniques have been introduced to support compression and clustering of data, which facilitate efficient data access. The combination of several factors – such as the query selectivity, the presence of dimension ordering – and the choice of the physical layout highly affects the performance of the query. For example, ordering on one dimension does not benefit queries that select other unordered columns. Data structures such as R-tree are limited by the curse of dimensionality and show degraded performance as the number of dimensions increases. By measuring these conditions, it is possible to move towards an approach that efficiently performs range selection of data, while maintaining stable performance for varying queries.
The goal of this project is to quantify whether multidimensional partitioning could be better or worse than a range-based partition in one dimension, and evaluate which factors affect the result.

The implementation includes:
- 7 multidimensional structures – Fixed-Grid, Grid-file, kd-tree, Quadtree, STRTree, Z-curve ordering, Hilbert curve 
- read, write and partitioning of data based on such structures.

Furthermore, for the benchmarking part:
- Design of a set of queries to use in the measurements. 
- Benchmarks computing the numbers of partitions retrieved by the queries across different structures, queries and partition size
