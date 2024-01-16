from plotly.subplots import make_subplots
from settings import DATASETS, PARTITIONINGS

import numpy as np
import os
import pandas as pd
import plotly.express as px

# Configuration
RESULTS_FOLDER = 'results/'
RESULTS_FILE = RESULTS_FOLDER + 'results.csv'
PLOTS_FOLDER = RESULTS_FOLDER + 'plots/'

# Parse csv results and clean rows
df = pd.read_csv(RESULTS_FILE, sep=';', on_bad_lines='skip',
                 converters={
                     "partitioning_columns": lambda x: x.strip("[]").replace("'", "").split(", "),
                     "used_columns": lambda x: x.strip("[]").replace("'", "").split(", ")
                 })
df.columns = df.columns.str.strip()
# Drop invalid data (measured latency is 0)
df = df[df.latency_avg != 0]
df['used_partitions'] = df[['used_partitions', 'total_partitions']].min(axis=1)

# Compute additional information
df['scan_ratio'] = (df['used_partitions'] / df['total_partitions']) * 100

df['column_match'] = [list(set(a).intersection(set(b)))
                      for a, b in zip(df['partitioning_columns'], df['used_columns'])]
df['column_match_ratio'] = (df['column_match'].str.len() / df['used_columns'].str.len()) * 100
# Discretize selectivity into intervals
selectivity_groups = [0.001, 0.01, 0.1, 0.5, 1, 2.5, 5]
rounding_logic = pd.Series(selectivity_groups)
labels = rounding_logic.tolist()
rounding_logic = pd.Series([-np.inf])._append(rounding_logic)  # add infinity as leftmost edge
df['selectivity_group'] = pd.cut(df['selectivity'], rounding_logic, labels=labels).fillna(rounding_logic.iloc[-1])

# Create subsets for specific analysis
df_partition_size = df.groupby(['partition_size', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()
df_selectivity = df.groupby(['selectivity', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()

# -- Define plots

effects = ['selectivity', 'partition_size', 'num_partitioning_columns']
metrics = ['latency_avg', 'scan_ratio']

# Figure 1: latency by dataset for all schemes: bar plot
plotGeneralLatency = make_subplots(rows=1, cols=len(DATASETS), column_titles=DATASETS,
                                   x_title='Scheme', y_title='Average query time (s)')
plotGeneralLatency.update_layout(title='Latency By Scheme')
# Figure 2: latency by dataset for all schemes with increasing partition size: line plot
plotLatencyVsPartitionSize = make_subplots(rows=1, cols=len(DATASETS), column_titles=DATASETS,
                                           x_title='Partition size', y_title='Average query time (s)')
plotLatencyVsPartitionSize.update_layout(title='Latency vs Partition Size')
# Figure 3: latency by dataset for all schemes with increasing selectivity: line plot
plotLatencyVsSelectivity = make_subplots(rows=1, cols=len(DATASETS), column_titles=DATASETS,
                                         x_title='Selectivity', y_title='Average query time (s)')
plotLatencyVsSelectivity.update_layout(title='Latency vs Selectivity')
# Figure 4: latency by dataset for all schemes with dataset size: line plot
plotLatencyVsDatasetSize = make_subplots(rows=1, cols=len(DATASETS), column_titles=DATASETS,
                                         x_title='Dataset size', y_title='Average query time (s)')
plotLatencyVsDatasetSize.update_layout(title='Latency vs Dataset Size')
# Figure 5: latency by dataset for all schemes with increasing partitioning columns: line plot
plotLatencyVsNumColumns = make_subplots(rows=1, cols=len(DATASETS), column_titles=DATASETS,
                                        x_title='Number of partitioning columns', y_title='Average query time (s)')
plotLatencyVsNumColumns.update_layout(title='Latency vs Number of Partitioning Columns')
# Figure 6: latency by dataset for all schemes with increasing partitioning columns: line plot
plotLatencyVsColumnMatch = make_subplots(rows=1, cols=len(DATASETS), column_titles=DATASETS,
                                         x_title='Ratio matching columns (partitioning columns / predicate columns) '
                                                 'of partitioning columns', y_title='Average query time (s)')
plotLatencyVsColumnMatch.update_layout(title='Latency vs Column Match Ratio')

# TODO
# Figure X: latency by dataset for all schemes at selectivity = 0.001
# Figure X: latency by dataset for all schemes at selectivity = 0.01
# Figure X: latency by dataset for all schemes at selectivity = 0.1

plots = [plotGeneralLatency, plotLatencyVsPartitionSize, plotLatencyVsSelectivity, plotLatencyVsDatasetSize,
         plotLatencyVsNumColumns, plotLatencyVsColumnMatch]

for i, dataset in enumerate(DATASETS):
    df_dataset = df[df['dataset'] == f'{dataset}']
    df_group_by_partitioning = df_dataset.groupby(['partitioning']).agg({'latency_avg': 'mean'}).reset_index()
    df_group_by_partition_size = df_dataset.groupby(['partition_size', 'partitioning']).agg(
        {'latency_avg': 'mean'}).reset_index()
    df_group_by_selectivity = df_dataset.groupby(['selectivity', 'partitioning']).agg(
        {'latency_avg': 'mean'}).reset_index()
    df_group_by_num_rows = df_dataset.groupby(['num_rows', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()
    df_group_by_num_cols = df_dataset.groupby(['num_partitioning_columns', 'partitioning']).agg(
        {'latency_avg': 'mean'}).reset_index()
    df_group_by_col_ratio = df_dataset.groupby(['column_match_ratio', 'partitioning']).agg(
        {'latency_avg': 'mean'}).reset_index()
    # Figure 1: latency by dataset for all schemes: bar plot
    plotLatency = px.histogram(df_dataset, x="partitioning", y="latency_avg", color="partitioning",
                               barmode="group", histfunc='avg', labels=PARTITIONINGS,
                               title=f'[{dataset}] Impact of the partitioning scheme on the query latency')
    for trace in range(len(plotLatency["data"])):
        plotGeneralLatency.add_trace(plotLatency["data"][trace], row=1, col=i + 1)

    # Figure 2: latency by dataset for all schemes with increasing partition size: line plot
    plotPartitionSize = px.line(df_group_by_partition_size,
                                x="partition_size", y="latency_avg", color="partitioning", markers=True,
                                title=f'[{dataset}] Impact of the partition size on the query latency')
    for trace in range(len(plotPartitionSize["data"])):
        plotLatencyVsPartitionSize.add_trace(plotPartitionSize["data"][trace], row=1, col=i + 1)
    # Figure 3: latency by dataset for all schemes with increasing selectivity: line plot
    plotSelectivity = px.line(df_group_by_selectivity,
                              x="selectivity", y="latency_avg", color="partitioning", markers=True,
                              title=f'[{dataset}] Impact of the selectivity on the query latency')
    for trace in range(len(plotSelectivity["data"])):
        plotLatencyVsSelectivity.add_trace(plotSelectivity["data"][trace], row=1, col=i + 1)
        plotSelectivity.update_layout(dict(
            xaxis=dict(
                tickmode='array',
                tickvals=selectivity_groups,
                ticktext=[str(selectivity) for selectivity in selectivity_groups]
            ),
            bargap=0
        ))
    # Figure 4: latency by dataset for all schemes with dataset size: line plot
    plotDatasetSize = px.line(df_group_by_num_rows,
                              x="num_rows", y="latency_avg", color="partitioning",
                              markers=True,
                              title=f'[{dataset}] Impact of the dataset size on the query latency')
    for trace in range(len(plotDatasetSize["data"])):
        plotLatencyVsDatasetSize.add_trace(plotDatasetSize["data"][trace], row=1, col=i + 1)
    # Figure 5: latency by dataset for all schemes with increasing partitioning columns: line plot
    plotNumColumns = px.line(df_group_by_num_cols,
                             x="num_partitioning_columns", y="latency_avg", color="partitioning",
                             markers=True,
                             title=f'[{dataset}] Impact of the number of partitioning columns on the query latency')
    for trace in range(len(plotNumColumns["data"])):
        plotLatencyVsNumColumns.add_trace(plotNumColumns["data"][trace], row=1, col=i + 1)
    # Figure 5: latency by dataset for all schemes with increasing column match ratio: line plot
    plotColumnRatio = px.line(df_group_by_col_ratio,
                              x="column_match_ratio", y="latency_avg", color="partitioning",
                              markers=True,
                              title=f'[{dataset}] Impact of the column match on the query latency')
    for trace in range(len(plotNumColumns["data"])):
        plotLatencyVsColumnMatch.add_trace(plotColumnRatio["data"][trace], row=1, col=i + 1)


def export_images():
    for plot in plots:
        names = set()
        plot.for_each_trace(
            lambda trace:
            trace.update(showlegend=False)
            if (trace.name in names) else names.add(trace.name))
        title = plot.layout.title.text
        image_path = os.path.join(PLOTS_FOLDER, f"{title}.png")
        plot.write_image(image_path, scale=3, width=1500, height=500)


if __name__ == '__main__':
    export_images()
