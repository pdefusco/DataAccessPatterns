import fsspec
import pandas
import os
import adlfs

fsspec_handle = fsspec.open('abfs://demo-azure-1/userdata1.parquet', account_name=os.environ['ACCOUNT_NAME'], account_key=os.environ['ACCOUNT_KEY'])

with fsspec_handle.open() as f:
    df = pandas.read_parquet(f)

df.head()


