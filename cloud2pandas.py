## Reading the files into Pandas and Using Pandas SQL


#Read data file from URI of default Azure Data Lake Storage Gen2
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential











from azure.storage.filedatalake import DataLakeServiceClient
import pyarrow.parquet as pq
import io
import os

client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]
tenant_id = os.environ["TENANT_ID"]
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

storage_account_name = "data@go01esu"

service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
    "https", storage_account_name), credential=credential)
file_system = 'mytestdir'
file_system_client = service_client.get_file_system_client(file_system)

file_path = ''
file_client = file_system_client.get_file_client(file_path)
data = file_client.download_file(0)
with io.BytesIO() as b:
    data.readinto(b)
    table = pq.read_table(b)
    print(table)



import pandas



#read csv file
df = pandas.read_csv("{}/mytestdir/userdata1.parquet".format(storage))

df = pandas.read_csv("abfs[s]://data@go01esu.dfs.core.windows.net/mytestdir/userdata1.parquet")

print(df)

#write csv file
data = pandas.DataFrame({'Name':['A', 'B', 'C', 'D'], 'ID':[20, 21, 19, 18]})
data.to_csv('abfs[s]://file_system_name@account_name.dfs.core.windows.net/file_path')



{}/mytestdir/userdata1.parquet".format(storage))