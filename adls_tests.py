import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings


import pandas as pd
from azure.datalake.store import core, lib, multithread
token = lib.auth()


adlsFileSystemClient = core.AzureDLFileSystem(token, store_name='amitadls')




# Read a file into pandas dataframe
with adlsFileSystemClient.open('abfs://data@go01esu.dfs.core.windows.net/mytestdir/userdata1.parquet', 'rb') as f:
    df = pd.read_csv(f) 

# Show the dataframe
df