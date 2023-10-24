import duckdb
import os
import requests

from workload import Workload


class OSMWorkload(Workload):

    def __init__(self):
        super().__init__()
        self.total_rows = None
        self.area = 'europe/germany/berlin'  # 'europe', 'europe/germany'

    def get_name(self):
        return 'osm'

    def get_table_name(self):
        return 'node'

    def generate_dataset(self, **params):
        self.area = params.get("area", self.area)

        filename = os.path.join(self.get_dataset_folder(), f'{self.area}-latest.osm.pbf')
        with open(filename, 'wb') as out_file:
            url = f'https://download.geofabrik.de/{self.area}-latest.osm.pbf'
            print(f'Requesting file: {url}')
            content = requests.get(url, stream=True).content
            out_file.write(content)

        os.system("cd /lib")
        os.system("git clone https://github.com/adrianulbona/osm-parquetizer.git")
        os.system("cd osm-parquetizer")
        os.system("mvn clean package")
        os.system(f"java -jar target/osm-parquetizer-1.0.1-SNAPSHOT.jar {filename}")
        os.system(f"rm -rf {filename}")
        os.system(f"rm -rf {filename}.relation.parquet")
        os.system(f"rm -rf {filename}.way.parquet")
        os.system(f"cp {filename}.node.parquet {self.get_dataset_folder()}")

        self.total_rows = len(duckdb.sql(f'SELECT * FROM read_parquet(\'{self.get_files_pattern()}\')'))

    def generate_queries(self):
        raise NotImplementedError

    def is_dataset_generated(self) -> bool:
        return os.path.exists(os.path.join(self.get_dataset_folder(), f'{self.area}-latest.osm.pbf.node.parquet'))
