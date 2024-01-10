import os
import re
import subprocess

from settings import DATA_FORMAT


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
