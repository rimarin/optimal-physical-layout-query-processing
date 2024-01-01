import pandas
import plotly.express as px


def load_results():
    df = pandas.read_csv('results/results.csv', delimiter=';', on_bad_lines='skip')
    df.columns = df.columns.str.strip()
    return df


def generate_plots(df):
    plot_selectivity(df)
    plot_filtered_columns(df)
    plot_number_partitioning_columns(df)
    plot_partition_size(df)
    plot_dataset_size(df)


def plot_selectivity(df):
    fig = px.box(df, x="selectivity", y="latency_avg", points="all")
    fig.show()


def plot_filtered_columns(df):
    pass


def plot_number_partitioning_columns(df):
    pass


def plot_partition_size(df):
    pass


def plot_dataset_size(df):
    pass


if __name__ == "__main__":
    df = load_results()
    generate_plots(df)

