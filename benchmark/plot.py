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
# Set a fixed (optimal) partition size
# df = df[df['partition_size'] == 100000]
# Convert latency to milliseconds
df['latency_avg'] = df['latency_avg'] * 1000

# Compute additional information
df['scan_ratio'] = (df['used_partitions'] / df['total_partitions']) * 100

# Compute workload type
df['workload_type'] = pd.Series(index=df.index)
df.loc[df['num_used_columns'] > df['num_partitioning_columns'], 'workload_type'] = "A"
df.loc[df['num_used_columns'] == df['num_partitioning_columns'], 'workload_type'] = "B"
df.loc[df['num_used_columns'] < df['num_partitioning_columns'], 'workload_type'] = "C"

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

aggregates = {}
for metric in metrics:
    aggregates[metric] = 'mean'

# Figure 1: latency by dataset for all schemes: bar plot
impact_scheme = make_subplots(rows=2, cols=1, column_titles=DATASETS,
                              x_title='Scheme', shared_yaxes=True, shared_xaxes=True)
impact_scheme.update_layout(title='Impact of partitioning scheme')
# Figure 2: latency by dataset for all schemes with increasing partition size: line plot
impact_partition_size = make_subplots(rows=2, cols=len(DATASETS), column_titles=DATASETS,
                                      x_title='Partition size', shared_yaxes=True, shared_xaxes=True)
impact_partition_size.update_layout(title='Impact of partition size')
# Figure 3: latency by dataset for all schemes with increasing selectivity: line plot
impact_selectivity = make_subplots(rows=2, cols=len(DATASETS), column_titles=DATASETS,
                                   x_title='Selectivity', shared_yaxes=True, shared_xaxes=True)
impact_selectivity.update_layout(title='Impact of selectivity')
# Figure 4: latency by dataset for all schemes with dataset size: line plot
impact_dataset_size = make_subplots(rows=2, cols=len(DATASETS), column_titles=DATASETS,
                                    x_title='Dataset size', shared_yaxes=True, shared_xaxes=True)
impact_dataset_size.update_layout(title='Impact of dataset size')
# Figure 5: latency by dataset for all schemes with increasing partitioning columns: line plot
impact_num_columns = make_subplots(rows=2, cols=len(DATASETS), column_titles=DATASETS,
                                   x_title='Number of partitioning columns', shared_yaxes=True, shared_xaxes=True)
impact_num_columns.update_layout(title='Impact of number of partitioning columns')
# Figure 6: latency by dataset for all schemes with increasing partitioning columns: line plot
impact_column_match = make_subplots(rows=2, cols=len(DATASETS), column_titles=DATASETS,
                                    x_title='Ratio matching columns (partitioning columns / predicate columns) '
                                            'of partitioning columns', shared_yaxes=True, shared_xaxes=True)
impact_column_match.update_layout(title='Impact of column match ratio')
# Figure 7: latency by dataset for all schemes with workload type: bar plot
impact_workload = make_subplots(rows=2, cols=len(DATASETS), column_titles=DATASETS,
                                x_title='Workload type', shared_yaxes=True, shared_xaxes=True)
impact_workload.update_layout(title='Impact of workload type')
# Figure 8: latency by dataset for column combinations
df['used_columns'] = df['used_columns'].apply(tuple)
df['partitioning_columns'] = df['partitioning_columns'].apply(tuple)
df_group_by_columns = df.groupby(['dataset', 'partitioning', 'num_used_columns', 'num_partitioning_columns']).agg(
    aggregates).reset_index()
impact_columns_latency = px.bar(df_group_by_columns, x="dataset", y="latency_avg", facet_col="num_partitioning_columns",
                                facet_row="num_used_columns", color="partitioning", labels=PARTITIONINGS,
                                barmode='group', category_orders={'partitioning': sorted(df['partitioning'].unique())},
                                title=f'Impact of the columns combination on latency')
impact_columns_scan_ratio = px.bar(df_group_by_columns, x="dataset", y="scan_ratio", facet_col="num_partitioning_columns",
                                   facet_row="num_used_columns", color="partitioning", labels=PARTITIONINGS,
                                   barmode='group',
                                   category_orders={'partitioning': sorted(df['partitioning'].unique())},
                                   title=f'Impact of the columns combination on scan ratio')

plots = [impact_scheme, impact_partition_size, impact_selectivity, impact_dataset_size,
         impact_num_columns, impact_column_match, impact_workload, impact_columns_latency, impact_columns_scan_ratio]

# Figure 1: latency by dataset for all schemes: bar plot
df_scheme = df.groupby(['partitioning', 'dataset']).agg(aggregates).reset_index()
for y, metric in enumerate(metrics):
    sub_plot = px.bar(df_scheme, x="dataset", y=metric, color="partitioning",
                      labels=PARTITIONINGS, barmode='group',
                      category_orders={'partitioning': sorted(df['partitioning'].unique())},
                      title=f'[{df}] Impact of the partitioning scheme on the {metric}').update_layout(
        yaxis_title=metric)
    for trace in range(len(sub_plot["data"])):
        impact_scheme.add_trace(sub_plot["data"][trace], row=y + 1, col=1)
        impact_scheme.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)

for i, dataset in enumerate(DATASETS):
    df_dataset = df[df['dataset'] == f'{dataset}']
    df_group_by_partitioning = df_dataset.groupby(['partitioning']).agg(aggregates).reset_index()
    df_group_by_partition_size = df_dataset.groupby(['partition_size', 'partitioning']).agg(
        aggregates).reset_index()
    df_group_by_selectivity = df_dataset.groupby(['selectivity', 'partitioning']).agg(
        aggregates).reset_index()
    df_group_by_num_rows = df_dataset.groupby(['num_rows', 'partitioning']).agg(aggregates).reset_index()
    df_group_by_num_cols = df_dataset.groupby(['num_partitioning_columns', 'partitioning']).agg(
        aggregates).reset_index()
    df_group_by_col_ratio = df_dataset.groupby(['column_match_ratio', 'partitioning']).agg(
        aggregates).reset_index()

    # Figure 2: latency by dataset for all schemes with increasing partition size: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_partition_size,
                           x="partition_size", y=metric, color="partitioning", markers=True, labels=PARTITIONINGS,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the partition size on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_partition_size.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_partition_size.update_yaxes(title_text=metric, row=y + 1, col=1)
    # Figure 3: latency by dataset for all schemes with increasing selectivity: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_selectivity,
                           x="selectivity", y=metric, color="partitioning", markers=True, labels=PARTITIONINGS,
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
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the dataset size on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_dataset_size.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_dataset_size.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 5: latency by dataset for all schemes with increasing partitioning columns: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_num_cols,
                           x="num_partitioning_columns", y=metric, color="partitioning",
                           markers=True, labels=PARTITIONINGS,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the number of partitioning columns on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_num_columns.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_num_columns.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 5: latency by dataset for all schemes with increasing column match ratio: line plot
    for y, metric in enumerate(metrics):
        sub_plot = px.line(df_group_by_col_ratio,
                           x="column_match_ratio", y=metric, color="partitioning",
                           markers=True, labels=PARTITIONINGS,
                           category_orders={'partitioning': sorted(df_dataset['partitioning'].unique())},
                           title=f'[{dataset}] Impact of the column match on the {metric}')
        for trace in range(len(sub_plot["data"])):
            impact_column_match.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_column_match.update_yaxes(title_text=metric, type="log", row=y + 1, col=1)
    # Figure 7: latency by dataset for all schemes with workload type: bar plot
    df_group_by_workload = df.groupby(['workload_type', 'partitioning', 'dataset']).agg(aggregates).reset_index()
    for y, metric in enumerate(metrics):
        sub_plot = px.bar(df_group_by_workload, x="workload_type", y=metric, color="partitioning",
                          labels=PARTITIONINGS, barmode='group',
                          category_orders={'partitioning': sorted(df['partitioning'].unique())},
                          title=f'[{df}] Impact of the workload type on the {metric}').update_layout(
            yaxis_title=metric)
        for trace in range(len(sub_plot["data"])):
            impact_workload.add_trace(sub_plot["data"][trace], row=y + 1, col=i + 1)
            impact_workload.update_yaxes(title_text=metric, type="log", row=y + 1, col=i + 1)


def export_images():
    for plot in plots:
        names = set()
        plot.for_each_trace(
            lambda trace:
            trace.update(showlegend=False)
            if (trace.name in names) else names.add(trace.name))
        title = plot.layout.title.text
        image_path = os.path.join(PLOTS_FOLDER, f"{title}.png")
        plot.write_image(image_path, scale=3, width=1500, height=1000)


if __name__ == '__main__':
    export_images()
