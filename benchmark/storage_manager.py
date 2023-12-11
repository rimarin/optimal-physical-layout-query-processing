import os
import re
import subprocess

from config import DATA_FORMAT


class StorageManager:

    def __init__(self, path=None):
        self.path = path

    @staticmethod
    def delete_files(path):
        if path == '' or path == '/' or path == '/home/':
            raise Exception('Probably you do not want to delete these folders')
        for filename in os.listdir(path):
            if filename.endswith(DATA_FORMAT):
                os.remove(os.path.join(path, filename))

    @staticmethod
    def get_num_files(path, extension=DATA_FORMAT):
        return len(StorageManager.get_files(path, extension))

    @staticmethod
    def get_files(path, extension=DATA_FORMAT):
        if extension:
            return [f for f in os.listdir(path) if f.endswith(extension)]
        return [f for f in os.listdir(path)]

    @staticmethod
    def get_disk_space(self):
        try:
            out = subprocess.run(
                ['df'], check=True, capture_output=True, text=True
            )
        except subprocess.SubprocessError as e:
            raise Exception(f'Failed to measure disk space: {e}')
        parsed = re.search(
            f'.* (\d+) .* (\d+)%.*{self.partition}\\n', out.stdout
        )
        if parsed is None:
            raise Exception('Failed to parse "df" output')
        return int(parsed.group(1)), int(parsed.group(2))  # available, use
