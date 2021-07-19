# READ AND WRITE FILES IN OBJECT STORAGE WITH CML

This project is a quickstart to show how to get OBJECT STORAGE URL and read/write files into it via Spark. It works for both S3 or ADLS2 without making changes. 


# INSTRUCTIONS FOR USE

1. Install requirements: run "pip3 install -r requirements.txt" in a Session (terminal or session prompt) 
    - use "!pip3 install -r requirements.txt" if in session prompt


2. Execute "read_cloud_storage_url.py" (you can run this in a session with "Workbench" editor option)
    - This will get the correct url from hive-site.xml
    - Then it will use CLI commands to put a small csv in a test dir
    
    
3. Execute "read_cloud_storage_spark.py" (you can run this in a session with "Workbench" editor option)
    - This will read the test csv file into a SparkSession and create a DataFrame
    - Then it will write the same file into another test dir with parquet format
    
    
4. You can use the CLI command at the end of get_adls2_url.py to remove the test files via CLI

