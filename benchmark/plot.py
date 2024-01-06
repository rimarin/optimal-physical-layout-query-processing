from dash import Dash, html, dcc
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.express as px

RESULTS_FOLDER = 'results/'
RESULTS_FILE = RESULTS_FOLDER + 'results-server.csv'

df = pd.read_csv(RESULTS_FILE, sep=';', on_bad_lines='skip')
df.columns = df.columns.str.strip()
df['scan_ratio'] = (df['used_partitions'] / df['total_partitions']) * 100

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# TODO: compute and add column scan ratio % (used_partitions / num_partitions * 100)
dfPartitionSize = df.groupby(['partition_size', 'dataset', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()
dfSelectivity = df.groupby(['selectivity', 'partitioning']).agg({'latency_avg': 'mean'}).reset_index()

app.layout = html.Div([
    html.H1(children='Optimal Physical Layout | Multi-dimensional partitioning'),
    html.H4(children='Effect of selectivity'),
    dbc.Row([
        dbc.Col([
            dcc.Graph(figure=px.box(df, x="selectivity", y="latency_avg", points="all", color="dataset",
                                    title='Impact of selectivity on query latency')),
        ], width=6),
        dbc.Col([
            dcc.Graph(figure=px.box(df, x="selectivity", y="scan_ratio", points="all", color="dataset",
                                    title='Impact of selectivity on fetched partitions')),
        ], width=6), ]),
    dcc.Graph(figure=px.line(dfSelectivity, x="selectivity", y="latency_avg", color="partitioning",
                             title='Impact of selectivity on the partitioning schemes')),
    html.H4(children='Effect of number of partitioning columns'),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'fixed-grid'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[Fixed Grid] Impact of the number of partitioning columns on the query latency')),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'grid-file'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[Grid File] Impact of the number of partitioning columns on the query latency')),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'kd-tree'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[KD Tree] Impact of the number of partitioning columns on the query latency')),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'quad-tree'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[Quad Tree] Impact of the number of partitioning columns on the query latency')),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'str-tree'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[STR Tree] Impact of the number of partitioning columns on the query latency')),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'hilbert-curve'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[Hilbert Curve] Impact of the number of partitioning columns on the query latency')),
    dcc.Graph(figure=px.box(df[df['partitioning'] == 'z-order-curve'], x="num_partitioning_columns", y="latency_avg",
                            points="all", color="dataset",
                            title='[Z-Order Curve] Impact of the number of partitioning columns on the query latency')),
    html.H4(children='Effect of filtered columns'),
    html.H4(children='Effect of partitioning technique'),
    dcc.Graph(figure=px.histogram(df, x="partitioning", y="latency_avg", color="dataset",
                                  title='Impact of partitioning techniques on the query latency')),
    html.H4(children='Effect of partition size'),
    dcc.Graph(figure=px.box(df, x="partition_size", y="latency_avg", points="all", color="dataset",
                            title='Impact of partition size on the latency')),
    dcc.Graph(figure=px.line(dfPartitionSize, x="partition_size", y="latency_avg", color="partitioning",
                             title='Impact of partition size on the latency, by technique')),
    html.H4(children='Effect of dataset size'),
    dcc.Graph(figure=px.box(df, x="num_rows", y="latency_avg", points="all", color="dataset",
                            title='Impact of dataset size on the query latency')),
], style={'font-family': 'Inter', 'text-align': 'center'})


if __name__ == '__main__':
    app.run(debug=True)
