from settings import DATASETS, PARTITIONINGS

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

# Create subsets for specific analysis
df_partition_size = df.groupby(['partition_size', 'dataset', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()
df_selectivity = df.groupby(['selectivity', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()

# -- Define plots

effects = ['selectivity', 'partition_size', 'num_partitioning_columns']
metrics = ['latency_avg', 'scan_ratio']
plots = []
for dataset in DATASETS:
    df_dataset = df[df['dataset'] == f'{dataset}']
    # Impact of selectivity on latency
    plots.append(px.box(df_dataset, x="selectivity", y="latency_avg", color="partitioning",
                        title=f'[{dataset}] Impact of selectivity on query latency'))
    for partitioning in PARTITIONINGS:
        plots.append(px.bar(df_dataset[df_dataset['partitioning'] == f'{partitioning}'],
                            x="selectivity", y="latency_avg", color="partitioning",
                            title=f'[{dataset}, {partitioning}] Impact of selectivity on query latency'))
    # Impact of scan ratio on latency
    plots.append(px.box(df_dataset, x="selectivity", y="scan_ratio", color="partitioning",
                        title=f'[{dataset}] Impact of selectivity on fetched partitions'))
    # Impact of number of partitioning columns
    plots.append(px.box(df_dataset, x="num_partitioning_columns", y="latency_avg", color="partitioning",
                        title=f'[{dataset}] Impact of the number of partitioning columns on the query latency'))
    # Impact of partition size
    plots.append(px.box(df_dataset, x="partition_size", y="latency_avg", color="partitioning",
                        title=f'[{dataset}] Impact of partition size on the latency'))
    # Impact of dataset size
    plots.append(px.box(df_dataset, x="num_rows", y="latency_avg", color="partitioning",
                        title=f'[{dataset}] Impact of dataset size on the query latency'))
    # Impact of column match ratio
    plots.append(px.box(df_dataset, x="column_match_ratio", y="latency_avg", color="partitioning",
                        title=f'[{dataset}] Impact of column match on the query latency'))

# (General) Impact of selectivity on latency
plots.append(px.histogram(df, x="dataset", y="latency_avg", barmode='group', color='partitioning',
                          title=f'Impact of selectivity on query latency'))


def export_images():
    for plot in plots:
        title = ''.join(plot.layout.title['text'])
        image_path = os.path.join(PLOTS_FOLDER, f"{title}.png")
        plot.write_image(image_path)


if __name__ == '__main__':
    export_images()
