#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 15:02:01 2022

Code mostly lifted from this official Microsoft sample, with some slight adjustments: 
https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-blob/samples/blob_samples_directory_interface.py

Note that blob tier is set to "Archive" in this script.

AZURE_STORAGE_CONNECTION_STRING is set as an environmental variable...
Run script by in terminal with:
"python3 <script_name> <container_name>"

(remember to change the value of the destination_folder variable before running...)

"""


import os
from azure.storage.blob import BlobServiceClient, StandardBlobTier

class DirectoryClient:
  def __init__(self, connection_string, container_name):
    service_client = BlobServiceClient.from_connection_string(connection_string)
    self.client = service_client.get_container_client(container_name)

  def upload(self, source, dest):
    '''
    Upload a file or directory to a path inside the container
    '''
    if (os.path.isdir(source)):
      self.upload_dir(source, dest)
    else:
      self.upload_file(source, dest)

  def upload_file(self, source, dest):
    '''
    Upload a single file to a path inside the container
    '''
    print(f'Uploading {source} to {dest}')
    blb_tier = StandardBlobTier("Archive")
    with open(source, 'rb') as data:
      self.client.upload_blob(name=dest, data=data, standard_blob_tier=blb_tier)

  def upload_dir(self, source, dest):
    '''
    Upload a directory to a path inside the container
    '''
    prefix = '' if dest == '' else dest + '/'
    prefix += os.path.basename(source) + '/'
    for root, dirs, files in os.walk(source):
      for name in files:
        dir_part = os.path.relpath(root, source)
        dir_part = '' if dir_part == '.' else dir_part + '/'
        file_path = os.path.join(root, name)
        blob_path = prefix + dir_part + name
        self.upload_file(file_path, blob_path)


  def ls_files(self, path, recursive=False):
    '''
    List files under a path, optionally recursively
    '''
    if not path == '' and not path.endswith('/'):
      path += '/'

    blob_iter = self.client.list_blobs(name_starts_with=path)
    files = []
    for blob in blob_iter:
      relative_path = os.path.relpath(blob.name, path)
      if recursive or not '/' in relative_path:
        files.append(relative_path)
    return files


  def rmdir(self, path):
    '''
    Remove a directory and its contents recursively
    '''
    blobs = self.ls_files(path, recursive=True)
    if not blobs:
      return

    if not path == '' and not path.endswith('/'):
      path += '/'
    blobs = [path + blob for blob in blobs]
    print(f'Deleting {", ".join(blobs)}')
    self.client.delete_blobs(*blobs)

# Sample setup

import sys
try:
  CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']
except KeyError:
  print('AZURE_STORAGE_CONNECTION_STRING must be set')
  sys.exit(1)

try:
  CONTAINER_NAME = sys.argv[1]
except IndexError:
  print('usage: directory_interface.py CONTAINER_NAME')
  print('error: the following arguments are required: CONTAINER_NAME')
  sys.exit(1)



##################### UPLOAD #####################

source_folder = input("Paste root directory from which to get folders/files:")
destination_folder = "photos_backup"

client = DirectoryClient(CONNECTION_STRING, CONTAINER_NAME)

# Upload a directory to the container with a path prefix. The directory
# structure will be preserved inside the path prefix.
client.upload(source_folder, destination_folder)
files = client.ls_files('', recursive=True)
print(files)


##################### DELETE #####################

# Delete files in a directory recursively.
# After this call, the container will be empty.
#UNCOMMENT BELOW (and make appropriate adjustments) TO DELETE FILES
# client.rmdir(destination_folder)
# files = client.ls_files('', recursive=True)
# print(files)


print("Completed!")