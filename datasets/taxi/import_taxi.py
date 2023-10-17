import requests

START_YEAR = 2009
END_YEAR = 2023
YEARS = [str(year) for year in range(START_YEAR, END_YEAR+1)]
MONTHS = [str(month).zfill(2) for month in range(1, 13)]

for year in YEARS:
    for month in MONTHS:
        filename = f'yellow_tripdata_{year}-{month}.parquet'
        with open(filename, 'wb') as out_file:
            url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}'
            print(f'Requesting file: {url}')
            content = requests.get(url, stream=True).content
            out_file.write(content)
