# -*- coding: utf-8 -*-
# @author: elson
# @file: test.py
# @time: 2023/6/10 15:08
import os

from os import path
import io
import zipfile
from azure.storage.blob import BlobServiceClient
import uuid


# os.system("pip install azure-storage-blob azure-identity")


class Compress_utils:
    def __init__(self, storage_account, container, file_path, compress_type, password=None):
        self.storage_account = storage_account
        self.container = container
        self.file_path = file_path
        self.compress_type = compress_type
        self.password = bytes(password.encode()) if password else r''

        self.conn_str = ""
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_client = self.blob_service_client.get_container_client(self.container)
        self.blob_client = self.container_client.get_blob_client(self.file_path)

    def decompress_blob(self):
        _UUID = str(uuid.UUID)
        with io.BytesIO() as b:
            stream = self.blob_client.download_blob(0)
            stream.readinto(b)
            if self.compress_type.lower() == 'zip':
                with zipfile.ZipFile(b, compression=zipfile.ZIP_LZMA) as z:
                    for filename in z.namelist():
                        if not filename.endswith('/'):
                            new_folder = path.join(path.dirname(self.file_path), path.basename(filename) + _UUID)
                            print(new_folder)
                            with z.open(filename, mode='r', pwd=self.password) as f:
                                self.container_client.get_blob_client(new_folder).upload_blob(f)
            else:
                raise NotImplementedError

    def compress_blob(self):
        pass


if __name__ == '__main__':
    Utils = Compress_utils(storage_account="storage_account",
                           container="container",
                           file_path="/unzip/test.zip",
                           compress_type="zip")
    Utils.decompress_blob()