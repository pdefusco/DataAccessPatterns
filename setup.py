import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime

#Extracting the correct URL from hive-site.xml
tree = ET.parse('/etc/hadoop/conf/hive-site.xml')
root = tree.getroot()

for prop in root.findall('property'):
    if prop.find('name').text == "hive.metastore.warehouse.dir":
        storage = prop.find('value').text.split("/")[0] + "//" + prop.find('value').text.split("/")[2]

print("The correct ADLS 2 URL is:{}".format(storage))

os.environ['STORAGE'] = storage

#Now some sample CLI commands to create a test dir and upload a file from CML 
#You can run these within a notebook, editor file, or in the session prompt (bottom right) with an exclamation mark
#Or you can run these in the terminal (top right) without the exclamation mark

!hdfs dfs -mkdir -p $STORAGE/mytestdir
!hdfs dfs -copyFromLocal /home/cdsw/data/test_file.csv $STORAGE/mytestdir/test_file.csv
!hdfs dfs -copyFromLocal /home/cdsw/data/userdata1.parquet $STORAGE/mytestdir/userdata1.parquet
!hdfs dfs -ls $STORAGE/mytestdir

#Optionally: remove the testdir:
#Run this command after you test the SparkSession in the spark_read_adls2.py file
#!hdfs dfs -rm -r $STORAGE/datalake/mytestdir