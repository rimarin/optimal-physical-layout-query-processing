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
OPTIMAL_PARTITION_SIZE = 250000

# Parse csv results and clean rows
df = pd.read_csv(RESULTS_FILE, sep=';', on_bad_lines='skip',
                 converters={
                     "partitioning_columns": lambda x: x.strip("[]").replace("'", "").split(", "),
                     "used_columns": lambda x: x.strip("[]").replace("'", "").split(", ")
                 })
df.columns = df.columns.str.strip()
# Drop duplicates
df = df.loc[df.astype(str).drop_duplicates().index]
# Drop invalid data (measured latency is 0). Possibly includes TIMEOUT errors
# df = df[df['latency_avg'] != 0]
df['latency_avg'] = df['latency_avg'].replace(0, np.NaN)
# Align updated data format, if necessary
if 'used_partitions' in df:
    df = df.rename(columns={'used_partitions': 'fetched_partitions'})
# Drop failed or incorrect partitioning. 1 partition could be possible only for very high partition sizes
df = df[(df['total_partitions'] != 0) & ~((df['total_partitions'] == 1) & (df['partition_size'] < 500000))]
df['fetched_partitions'] = df[['fetched_partitions', 'total_partitions']].min(axis=1)
# Set a fixed (optimal) partition size
# df = df[df['partition_size'] == 100000]
df = df[df['num_partitioning_columns'] != 2]
df = df[df['num_used_columns'] <= 7]
# Convert latency to milliseconds
df['latency_avg'] = df['latency_avg'] * 1000

df['dataset'] = pd.Categorical(df['dataset'], ordered=True, categories=DATASETS)
df = df.sort_values('dataset')

# Compute additional information
df['scan_ratio'] = (df['fetched_partitions'] / df['total_partitions']) * 100

# Compute workload type
df['workload_type'] = pd.Series(index=df.index)
df['workload_type'] = df['workload_type'].astype('string')
# Fewer dims than indexed
df.loc[df['num_used_columns'] > df['num_partitioning_columns'], 'workload_type'] = "FD"
# As many as indexed
df.loc[df['num_used_columns'] == df['num_partitioning_columns'], 'workload_type'] = "AD"
# More than indexed
df.loc[df['num_used_columns'] < df['num_partitioning_columns'], 'workload_type'] = "MD"

df['column_match'] = [list(set(a).intersection(set(b)))
                      for a, b in zip(df['partitioning_columns'], df['used_columns'])]
df['column_match_ratio'] = (df['column_match'].str.len() / df['used_columns'].str.len()) * 100

# Discretize selectivity into intervals
selectivity_groups = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5]
rounding_logic = pd.Series(selectivity_groups)
labels = rounding_logic.tolist()
rounding_logic = pd.Series([-np.inf])._append(rounding_logic)  # add infinity as leftmost edge
df['selectivity_group'] = pd.cut(df['selectivity'], rounding_logic, labels=labels).fillna(rounding_logic.iloc[-1])

# -- Define plots

# https://colorbrewer2.org/#type=qualitative&scheme=Set1&n=8
partitioning_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#a65628', '#f781bf']

effects = ['selectivity', 'partition_size', 'num_partitioning_columns']
metrics = ['latency_avg', 'scan_ratio', 'fetched_row_groups', 'fetched_rows']

aggregates = {}
for metric in metrics:
    aggregates[metric] = 'mean'

# Figure 1: latency by dataset for all schemes: bar plot
impact_scheme = make_subplots(rows=len(metrics), cols=1,
                              x_title='Scheme', shared_yaxes=True, shared_xaxes=True)
impact_scheme.update_layout(title='Impact of partitioning scheme')
# Figure 2: latency by dataset for all schemes with increasing partition size: line plot
impact_partition_size = make_subplots(rows=len(metrics), cols=len(DATASETS), column_titles=DATASETS,
                                      x_title='Partition size', shared_yaxes=True, shared_xaxes=True)
impact_partition_size.update_layout(title='Impact of partition size')
# Figure 3: latency by dataset for all schemes with increasing selectivity: line plot
impact_selectivity = make_subplots(rows=len(metrics), cols=len(DATASETS), column_titles=DATASETS,
                                   x_title='Selectivity', shared_yaxes=True, shared_xaxes=True)
impact_selectivity.update_layout(title='Impact of selectivity')
# Figure 4: latency by dataset for all schemes with dataset size: line plot
impact_dataset_size = make_subplots(rows=len(metrics), cols=len(DATASETS), column_titles=DATASETS,
                                    x_title='Dataset size', shared_yaxes=True, shared_xaxes=True)
impact_dataset_size.update_layout(title='Impact of dataset size')
# Figure 5: latency by dataset for all schemes with increasing partitioning columns: line plot
impact_num_columns = make_subplots(rows=len(metrics), cols=len(DATASETS), column_titles=DATASETS,
                                   x_title='Number of partitioning columns', shared_yaxes=True, shared_xaxes=True)
impact_num_columns.update_layout(title='Impact of number of partitioning columns')
# Figure 6: latency by dataset for all schemes with increasing partitioning columns: line plot
impact_column_match = make_subplots(rows=len(metrics), cols=len(DATASETS), column_titles=DATASETS,
                                    x_title='Ratio matching columns (partitioning columns / predicate columns) '
                                            'of partitioning columns', shared_yaxes=True, shared_xaxes=True)
impact_column_match.update_layout(title='Impact of column match ratio')
# Figure 7: latency by dataset for all schemes with workload type: bar plot
impact_workload = make_subplots(rows=len(metrics), cols=len(DATASETS), column_titles=DATASETS,
                                x_title='Workload type', shared_yaxes=True, shared_xaxes=True)
impact_workload.update_layout(title='Impact of workload type')
# Figure 8: latency by dataset for column combinations
df['used_columns'] = df['used_columns'].apply(tuple)
df['partitioning_columns'] = df['partitioning_columns'].apply(tuple)
df_group_by_columns = df.groupby(['dataset', 'partitioning', 'num_used_columns', 'num_partitioning_columns'], observed=True).agg(
    aggregates).reset_index()
df_group_by_columns = df_group_by_columns.sort_values(by=['num_partitioning_columns', 'num_used_columns'],
                                                      ascending=True)
impact_columns_scan_ratio = px.bar(df_group_by_columns, x="dataset", y="scan_ratio",
                                   facet_col="num_partitioning_columns",
                                   facet_row="num_used_columns", color="partitioning", labels=PARTITIONINGS,
                                   barmode='group', color_discrete_sequence=partitioning_colors,
                                   category_orders={
                                       'partitioning': sorted(df['partitioning'].unique()),
                                       'num_partitioning_columns': sorted(df['num_partitioning_columns'].unique()),
                                       'num_used_columns': sorted(df['num_used_columns'].unique())
                                   },
                                   title=f'Impact of the columns combination on scan ratio')
impact_columns_row_groups = px.bar(df_group_by_columns, x="dataset", y="fetched_row_groups",
                                   facet_col="num_partitioning_columns",
                                   facet_row="num_used_columns", color="partitioning", labels=PARTITIONINGS,
                                   barmode='group', color_discrete_sequence=partitioning_colors,
                                   category_orders={
                                       'partitioning': sorted(df['partitioning'].unique()),
                                       'num_partitioning_columns': sorted(df['num_partitioning_columns'].unique()),
                                       'num_used_columns': sorted(df['num_used_columns'].unique())
                                   },
                                   title=f'Impact of the columns combination on fetched row groups')
impact_columns_rows = px.bar(df_group_by_columns, x="dataset", y="fetched_rows", facet_col="num_partitioning_columns",
                             facet_row="num_used_columns", color="partitioning", labels=PARTITIONINGS,
                             barmode='group', color_discrete_sequence=partitioning_colors,
                             category_orders={
                                 'partitioning': sorted(df['partitioning'].unique()),
                                 'num_partitioning_columns': sorted(df['num_partitioning_columns'].unique()),
                                 'num_used_columns': sorted(df['num_used_columns'].unique())
                             },
                             title=f'Impact of the columns combination on fetched rows')
impact_columns_latency = px.bar(df_group_by_columns, x="dataset", y="latency_avg", facet_col="num_partitioning_columns",
                                facet_row="num_used_columns", color="partitioning", labels=PARTITIONINGS,
                                color_discrete_sequence=partitioning_colors, barmode='group',
                                category_orders={
                                    'partitioning': sorted(df['partitioning'].unique()),
                                    'num_partitioning_columns': sorted(df['num_partitioning_columns'].unique()),
                                    'num_used_columns': sorted(df['num_used_columns'].unique())
                                },
                                title=f'Impact of the columns combination on latency')
df_group_by_time = df.groupby(['dataset', 'partitioning', 'num_used_columns', 'num_partitioning_columns',
                               'time_to_partition'], observed=True).agg(aggregates).reset_index()
df_group_by_time = df_group_by_time.sort_values(by=['num_partitioning_columns', 'num_used_columns'], ascending=True)
impact_partitioning_time = px.bar(df_group_by_time, x="dataset", y="time_to_partition",
                                  facet_col="num_partitioning_columns",
                                  color="partitioning", labels=PARTITIONINGS,
                                  color_discrete_sequence=partitioning_colors, barmode='group',
                                  category_orders={
                                      'partitioning': sorted(df['partitioning'].unique()),
                                      'num_partitioning_columns': sorted(df['num_partitioning_columns'].unique())
                                  },
                                  title=f'Impact of the partitioning time')
for y, indexed_cols in enumerate(df['num_partitioning_columns'].unique()):
    impact_partitioning_time.update_yaxes(type="log", row=1, col=y + 1)

plots = [impact_scheme, impact_partition_size, impact_selectivity, impact_dataset_size,
         impact_num_columns, impact_column_match, impact_workload, impact_partitioning_time]
plots_facet = [impact_columns_latency, impact_columns_scan_ratio, impact_columns_row_groups, impact_columns_rows]

# Figure 1: latency by dataset for all schemes: bar plot
df_scheme = df.groupby(['partitioning', 'dataset'], observed=True).agg(aggregates).reset_index()
for y, metric in enumerate(metrics):
    sub_plot = px.bar(df_scheme, x="dataset", y=metric, color="partitioning",
                      labels=PARTITIONINGS, barmode='group',
                      color_discrete_sequence=partitioning_colors,
                      category_orders={'partitioning': sorted(df['partitioning'].unique()), 'dataset': DATASETS},
                      title=f'[{df}] Impact of the partitioning scheme on the {metric}').update_layout(
        yaxis_title=metric)
    for trace in range(len(sub_plot["data"])):
        impact_scheme.add_trace(sub_plot["data"][trace], row=y + 1, col=1)
        impact_scheme.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)

for i, dataset in enumerate(DATASETS):
    df_dataset = df[df['dataset'] == f'{dataset}']
    df_group_by_partitioning = df_dataset.groupby(['partitioning'], observed=True).agg(aggregates).reset_index()
    df_group_by_partition_size = df_dataset.groupby(['partition_size', 'partitioning'], observed=True).agg(
        aggregates).reset_index()
    df_group_by_selectivity = df_dataset.groupby(['selectivity_group', 'partitioning'], observed=True).agg(aggregates).reset_index()
    df_group_by_num_rows = df_dataset.groupby(['num_rows', 'partitioning'], observed=True).agg(aggregates).reset_index()
    df_group_by_num_cols = df_dataset.groupby(['num_partitioning_columns', 'partitioning'], observed=True).agg(
        aggregates).reset_index()
    df_group_by_col_ratio = df_dataset.groupby(['column_match_ratio', 'partitioning'], observed=True).agg(
        aggregates).reset_index()
    df_group_by_workload = df_dataset.groupby(['workload_type', 'partitioning'], observed=True).agg(aggregates).reset_index()

    # Figure 2: latency by dataset for all schemes with increasing partition size: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_partition_size,
                           x="partition_size", y=metric, color="partitioning", markers=True, labels=PARTITIONINGS,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           color_discrete_sequence=partitioning_colors,
                           title=f'[{dataset}] Impact of the partition size on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_partition_size.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_partition_size.update_yaxes(title_text=metric, row=y + 1, col=1)
    # Figure 3: latency by dataset for all schemes with increasing selectivity: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_selectivity,
                           x="selectivity_group", y=metric, color="partitioning", markers=True, labels=PARTITIONINGS,
                           color_discrete_sequence=partitioning_colors,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the selectivity on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_selectivity.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            sub_plot.update_layout(dict(
                xaxis=dict(
                    tickmode='array',
                    tickvals=selectivity_groups,
                    ticktext=[str(selectivity) for selectivity in selectivity_groups]
                ),
                bargap=0
            ))
            impact_selectivity.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 4: latency by dataset for all schemes with dataset size: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_num_rows,
                           x="num_rows", y=metric, color="partitioning", markers=True, labels=PARTITIONINGS,
                           color_discrete_sequence=partitioning_colors,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the dataset size on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_dataset_size.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_dataset_size.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 5: latency by dataset for all schemes with increasing partitioning columns: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_num_cols,
                           x="num_partitioning_columns", y=metric, color="partitioning",
                           markers=True, labels=PARTITIONINGS, color_discrete_sequence=partitioning_colors,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the number of partitioning columns on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_num_columns.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_num_columns.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 5: latency by dataset for all schemes with increasing column match ratio: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_col_ratio,
                           x="column_match_ratio", y=metric, color="partitioning",
                           markers=True, labels=PARTITIONINGS, color_discrete_sequence=partitioning_colors,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the column match on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_column_match.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_column_match.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 7: latency by dataset for all schemes with workload type: bar plot
    for y, metric in enumerate(metrics):
        sub_plot = px.bar(df_group_by_workload, x="workload_type", y=metric, color="partitioning",
                          labels=PARTITIONINGS, barmode='group', color_discrete_sequence=partitioning_colors,
                          category_orders={'partitioning': sorted(df['partitioning'].unique())},
                          title=f'[{df}] Impact of the workload type on the {metric}').update_layout(
            yaxis_title=metric)
        for trace in range(len(sub_plot["data"])):
            impact_workload.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_workload.update_yaxes(title_text=metric, type="log", row=y + 1, col=i + 1)


def export_images():
    def export_plot(plot, width=1500, height=1000):
        names = set()
        plot.for_each_trace(
            lambda trace:
            trace.update(showlegend=False)
            if (trace.name in names) else names.add(trace.name))
        title = plot.layout.title.text
        if not os.path.exists(PLOTS_FOLDER):
            os.makedirs(PLOTS_FOLDER)
        image_path = os.path.join(PLOTS_FOLDER, f"{title}.png")
        plot.write_image(image_path, scale=3, width=width, height=height)

    for _plot in plots:
        export_plot(_plot)
    for _facet_plot in plots_facet:
        export_plot(_facet_plot, 1800, 1800)


if __name__ == '__main__':
    export_images()
