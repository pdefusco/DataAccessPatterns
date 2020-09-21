import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .getOrCreate()

#.master("local[*]")\
    
# **Note:** 
# Our file isn't big, so running it in Spark local mode is fine but you can add the following config 
# if you want to run Spark on the kubernetes cluster 
# 
# > .config("spark.yarn.access.hadoopFileSystems",os.getenv['STORAGE'])\

#.config("spark.authenticate", "true") \
#    .config("spark.yarn.access.hadoopFileSystems", os.environ['STORAGE'])\
#
# and remove `.master("local[*]")\`

storage = os.environ['STORAGE']
    
testDF = spark.read.csv("{}/mytestdir/test_file.csv".format(storage),             
  header=True,
  sep=','
)

#Checking the file has been loaded correctly
testDF.show()

#Checking the file can be written correctly from SparkSession

testDF.write.mode("overwrite").parquet("{}/mytestdir/parquetfile/test".format(storage))

!hdfs dfs -ls $STORAGE/mytestdir/parquetfile/test

#spark.stop()
#.config("spark.authenticate", "true") \
#.config("spark.yarn.access.hadoopFileSystems",os.environ['STORAGE'])\
