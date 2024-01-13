from dash import Dash, html, dcc

import dash_bootstrap_components as dbc
import os
import pandas as pd
import plotly.express as px

# Configuration
RESULTS_FOLDER = 'results/'
RESULTS_FILE = RESULTS_FOLDER + 'results.csv'
PLOTS_FOLDER = RESULTS_FOLDER + 'plots/'

# Parse csv results and clean rows
df = pd.read_csv(RESULTS_FILE, sep=';', on_bad_lines='skip')
df.columns = df.columns.str.strip()
# Drop invalid data (measured latency is 0)
df = df[df.latency_avg != 0]
df['used_partitions'] = df[['used_partitions', 'total_partitions']].min(axis=1)

# Compute additional information
df['scan_ratio'] = (df['used_partitions'] / df['total_partitions']) * 100

# Create subsets for specific analysis
df_partition_size = df.groupby(['partition_size', 'dataset', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()
df_selectivity = df.groupby(['selectivity', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()

# Define plots
figSelectivityLatency = px.box(df, x="selectivity", y="latency_avg", points="all", color="dataset",
                               title='Impact of selectivity on query latency')
figSelectivityScanRatio = px.box(df, x="selectivity", y="scan_ratio", points="all", color="dataset",
                                 title='Impact of selectivity on fetched partitions')
figSelectivitySchemes = px.line(df_selectivity, x="selectivity", y="latency_avg", color="partitioning",
                                title='Impact of selectivity on the partitioning schemes')
figNumColumns = px.box(df, x="num_partitioning_columns", y="latency_avg",
                       points="all", color="dataset",
                       title='Impact of the number of partitioning columns on the query latency')
figNumColumnsFixedGrid = px.box(df[df['partitioning'] == 'fixed-grid'], x="num_partitioning_columns", y="latency_avg",
                                points="all", color="dataset",
                                title='[Fixed Grid] Impact of the number of partitioning columns on the query latency')
figNumColumnsGridFile = px.box(df[df['partitioning'] == 'grid-file'], x="num_partitioning_columns", y="latency_avg",
                               points="all", color="dataset",
                               title='[Grid File] Impact of the number of partitioning columns on the query latency')
figNumColumnsKDTree = px.box(df[df['partitioning'] == 'kd-tree'], x="num_partitioning_columns", y="latency_avg",
                             points="all", color="dataset",
                             title='[KD Tree] Impact of the number of partitioning columns on the query latency')
figNumColumnsQuadTree = px.box(df[df['partitioning'] == 'quad-tree'], x="num_partitioning_columns", y="latency_avg",
                               points="all", color="dataset",
                               title='[Quad Tree] Impact of the number of partitioning columns on the query latency')
figNumColumnsSTRTree = px.box(df[df['partitioning'] == 'str-tree'], x="num_partitioning_columns", y="latency_avg",
                              points="all", color="dataset",
                              title='[STR Tree] Impact of the number of partitioning columns on the query latency')
figNumColumnsHilbertCurve = px.box(df[df['partitioning'] == 'hilbert-curve'], x="num_partitioning_columns",
                                   y="latency_avg",
                                   points="all", color="dataset",
                                   title='[Hilbert Curve] Impact of the number of partitioning columns on the query '
                                         'latency')
figNumColumnsZOrderCurve = px.box(df[df['partitioning'] == 'z-order-curve'], x="num_partitioning_columns",
                                  y="latency_avg",
                                  points="all", color="dataset",
                                  title='[Z-Order Curve] Impact of the number of partitioning columns on the query '
                                        'latency')
figSchemeLatency = px.histogram(df, x="partitioning", y="latency_avg", color="dataset",
                                title='Impact of partitioning schemes on the query latency')
figPartitionSizeLatency = px.box(df, x="partition_size", y="latency_avg", points="all", color="dataset",
                                 title='Impact of partition size on the latency')
figPartitionSizeLatencyByScheme = px.line(df_partition_size, x="partition_size", y="latency_avg", color="partitioning",
                                          title='Impact of partition size on the latency, by scheme')
figDatasetSizeLatency = px.box(df, x="num_rows", y="latency_avg", points="all", color="dataset",
                               title='Impact of dataset size on the query latency')

figures = [figSelectivityLatency, figSelectivityScanRatio, figSelectivitySchemes, figNumColumns,
           figNumColumnsFixedGrid, figNumColumnsGridFile, figNumColumnsKDTree, figNumColumnsQuadTree,
           figNumColumnsSTRTree, figNumColumnsHilbertCurve, figNumColumnsZOrderCurve,
           figSchemeLatency, figPartitionSizeLatency, figPartitionSizeLatencyByScheme, figDatasetSizeLatency]

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H1(children='Optimal Physical Layout | Multi-dimensional partitioning'),
    html.H4(children='Effect of selectivity'),
    dbc.Row([
        dbc.Col([dcc.Graph(figure=figSelectivityLatency), ], width=6),
        dbc.Col([dcc.Graph(figure=figSelectivityScanRatio), ], width=6),
    ]),
    dcc.Graph(figure=figSelectivitySchemes),
    html.H4(children='Effect of number of partitioning columns'),
    dcc.Graph(figure=figNumColumns),
    dcc.Graph(figure=figNumColumnsFixedGrid),
    dcc.Graph(figure=figNumColumnsGridFile),
    dcc.Graph(figure=figNumColumnsKDTree),
    dcc.Graph(figure=figNumColumnsQuadTree),
    dcc.Graph(figure=figNumColumnsSTRTree),
    dcc.Graph(figure=figNumColumnsHilbertCurve),
    dcc.Graph(figure=figNumColumnsZOrderCurve),
    html.H4(children='Effect of filtered columns'),
    html.H4(children='Effect of partitioning scheme'),
    dcc.Graph(figure=figSchemeLatency),
    html.H4(children='Effect of partition size'),
    dcc.Graph(figure=figPartitionSizeLatency),
    dcc.Graph(figure=figPartitionSizeLatencyByScheme),
    html.H4(children='Effect of dataset size'),
    dcc.Graph(figure=figDatasetSizeLatency),
], style={'font-family': 'Inter', 'text-align': 'center'})


def export_images():
    for fig in figures:
        title = ''.join(fig.layout.title['text'])
        image_path = os.path.join(PLOTS_FOLDER, f"{title}.png")
        fig.write_image(image_path)


if __name__ == '__main__':
    export_images()
    app.run(debug=True)
