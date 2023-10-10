import csv
import duckdb


def convert_to_csv(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'a', newline='') as outfile:
        writer = csv.writer(outfile)
        for line in infile:
            values = [val.strip() for val in line.split()]
            writer.writerow(values)


def convert_to_parquet(input_file, output_file):
    # the dataset has 149485603 rows
    # be aware of this: https://github.com/duckdb/duckdb/issues/2816, https://duckdb.org/docs/data/parquet/tips
    # 122880 rows (which is the default batch size in the Parquet writer) should fit into memory
    # This does not help: PRAGMA temp_directory='genome.tmp'
    # set memory_limit="4GB";
    # set temp_directory="/home/brancaleone/tmp";
    # set threads=1;
    # set temp_directory = '/home/brancaleone/Downloads/genometmp';
    # duckdb.sql(f'COPY (SELECT * FROM read_csv_auto(\'{input_file}\')) TO \'{output_file}\' (FORMAT \'PARQUET\', ROW_GROUP_SIZE 50000)')
    duckdb.sql('set memory_limit="4GB"')
    duckdb.sql('set temp_directory="/home/brancaleone/tmp"')
    duckdb.sql('set threads=1')
    total_rows = 149485603
    chunk_size = 10000000
    num_chunks = int(total_rows / chunk_size)
    start = 0
    for i in range(start, num_chunks+1):
        output_file_part = output_file.split('.')[0] + str(i) + '.' + output_file.split('.')[1]
        duckdb.sql(f'CREATE TABLE genome{str(i)} AS SELECT * FROM read_csv_auto(\'{input_file}\') LIMIT {chunk_size} OFFSET {i*chunk_size}');
        duckdb.sql(f'COPY genome{str(i)} TO \'{output_file_part}\' (FORMAT PARQUET)')
        duckdb.sql(f'DROP TABLE genome{str(i)}')
        print(f'Exported to {output_file_part}')


if __name__ == "__main__":
    input_file = "chr22_feature.vectors"
    output_file = "genome.csv"

    convert_to_csv(input_file, output_file)
    print("1000genome feature vectors converted to CSV file", output_file)
    output_file_parquet = output_file.split('.')[0] + '.parquet'
    convert_to_parquet(output_file, output_file_parquet)
    print("CSV file converted to parquet", output_file_parquet)
