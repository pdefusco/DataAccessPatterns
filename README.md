# Cloudera Machine Learning Quickstart

## Summary

This project is a quickstart to show how to connect to S3 and ADLS with or without Spark from CML. 

- The "cloud2spark.py" script works for both S3 or ADLS2 without making changes. 
- The "adls2pandas.py" script is designed to read from ADLS2.


# Steb by Step Instructions 


0. Create ADLS Environment Variables

- Log into your Azure account and copy Access Key and Storage Account Name
- Navigate to Project Settings -> Advanced -> and add two environment variables as shown below

![alt-text]("img/env_vars.png)


1. Install requirements: 

- Open a CML Session with Python 3.6+ and Spark Runtime enabled (2 or 3 makes no difference) and Workbench editor

![alt-text]("img/startcmlsession.png)


- Run the "!pip3 install -r requirements.txt" command in the interactive Python prompt located at the bottom right or in the terminal (no need for the exclamation mark to prefix the command if running from terminal)

![alt-text]("img/pipinstall.png)


2. Execute "setup.py" (you can run this in a session with "Workbench" editor option)
    - This will get the correct url from hive-site.xml and save the ADLS URL to a temporary variable called "storage"
    - Then it will use the HDFS CLI to move data from the data folder into an ADLS container 
    
![alt-text]("img/executecode.png)
    
    
3. Execute "cloud2spark.py"
    - This will read the test csv file into a SparkSession and create a DataFrame
    - Then it will write the same file into another test dir with parquet format

![alt-text]("img/read_spark.png)   

        
4. Execute "adls2pandas.py" to read the parquet file into Pandas directly from ADLS 

![alt-text]("img/read_from_adls.png)