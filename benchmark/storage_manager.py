import os

from pathlib import Path


class StorageManager:

    def __init__(self, path=None):
        self.path = path

    def delete_files(self, path=None):
        if path:
            deletion_path = path
        elif self.path:
            deletion_path = self.path
        else:
            raise Exception('Path not provided')
        for path in Path(deletion_path).glob("*.parquet"):
            if path.is_file():
                path.unlink()
        print(f'Deleted files from {deletion_path}')

    @staticmethod
    def get_num_files(path=None, extension=None):
        return len(StorageManager.get_files(path, extension))

    @staticmethod
    def get_files(path=None, extension=None):
        if extension:
            return [f for f in os.listdir(path) if f.endswith(extension)]
        return [f for f in os.listdir(path)]
