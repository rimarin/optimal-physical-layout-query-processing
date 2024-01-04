from dash import Dash, html, dcc

import pandas as pd
import plotly.express as px

df = pd.read_csv('results/results.csv', sep=';', on_bad_lines='skip')
df.columns = df.columns.str.strip()

app = Dash(__name__)

app.layout = html.Div([
    html.H1(children='Optimal Physical Layout | Multi-dimensional partitioning', style={'textAlign': 'center'}),
    html.H4(children='Effect of selectivity', style={'textAlign': 'center'}),
    dcc.Graph(figure=px.box(df, x="selectivity", y="latency_avg", points="all", color="dataset")),
    html.H4(children='Effect of number of partitioning columns', style={'textAlign': 'center'}),
    dcc.Graph(figure=px.box(df, x="num_partitioning_columns", y="latency_avg", points="all", color="dataset")),
    html.H4(children='Effect of filtered columns', style={'textAlign': 'center'}),
    html.H4(children='Effect of partition size', style={'textAlign': 'center'}),
    dcc.Graph(figure=px.box(df, x="partition_size", y="latency_avg", points="all", color="dataset")),
    html.H4(children='Effect of dataset size', style={'textAlign': 'center'}),
    dcc.Graph(figure=px.box(df, x="num_rows", y="latency_avg", points="all", color="dataset")),
], style={'font-family': 'Inter'})


if __name__ == '__main__':
    app.run(debug=True)
