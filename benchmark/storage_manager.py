import os

from pathlib import Path

from config import DATA_FORMAT


class StorageManager:

    def __init__(self, path=None):
        self.path = path

    @staticmethod
    def delete_files(path):
        for deletion_path in Path(path).glob("*.parquet"):
            if deletion_path.is_file():
                deletion_path.unlink()
        print(f'Deleted files from {path}')

    @staticmethod
    def get_num_files(path, extension=DATA_FORMAT):
        return len(StorageManager.get_files(path, extension))

    @staticmethod
    def get_files(path, extension=DATA_FORMAT):
        if extension:
            return [f for f in os.listdir(path) if f.endswith(extension)]
        return [f for f in os.listdir(path)]
