# Databricks notebook source
# Need to install databricks-cli to access secrets via CLI
# %sh
# pip install databricks-cli

# This just hangs for minutes
# %sh
# databricks configure --token

# Error: InvalidConfigurationError: You haven't configured the CLI yet! Please configure by entering `/databricks/python3/bin/databricks configure`
# %sh
# databricks secrets list-scopes

# COMMAND ----------

mountPoint = "/mnt/blobmount"
loadPath   = "/example/data/users.csv"
savePath   = "/example/data/users/incremental"

# Check if blob storage is already mounted. If not, mount.
if mountPoint in [mp.mountPoint for mp in dbutils.fs.mounts()]:
    print(mountPoint + " exists!")
else:
  dbutils.fs.mount(
  source        = "wasbs://dv-hdinsight-2020-03-30t16-29-59-717z@dvhdinsighthdistorage.blob.core.windows.net",
  mount_point   = mountPoint,
  extra_configs = {"fs.azure.account.key.dvhdinsighthdistorage.blob.core.windows.net":dbutils.secrets.get(scope = "dv-db-blob-scope-name", key = "dv-db-blob-secret")})

# COMMAND ----------

"""
Reads data from '/mnt/blobmount/example/data/users.csv' folder
TODO: Allow this to read parquet as well

Paramters:
  -sFilePath(--string): In this format: 'csv/userdata1.csv'
  
Returns: 
  pyspark.sql.dataframe.DataFrame
"""
def readAndShowCSV(sFilePath):
  csvFile = spark.read.csv(mountPoint+loadPath+'/' + sFilePath, header=True, inferSchema=True)
  csvFile.show(5)
  
  return csvFile


"""
After saving a .csv or .parquet file to the incremental folder, read it back.
Reads data from "/mnt/blobmount/example/data/users/incremental"

Parameters:
  -sDate (--string): In this format: '31-03-2020_19-08'
  
Returns:
  pyspark.sql.dataframe.DataFrame
"""
def readIncrementalParquetFile(sDate): 
  parquetFile = spark.read.parquet(mountPoint + savePath + '/' + sDate + '.parquet')
  parquetFile.show(5)
  
  return parquetFile

# COMMAND ----------

def createTimeStamp(): 
    from datetime import datetime

    # datetime object containing current date and time
    now = datetime.now()

    # /dd-mm-YY_H:M
    dt_string = now.strftime("/%d-%m-%Y_%H-%M")    
    
    return dt_string
  

"""
Add time stamp to file path to prevent saving over orignial file.
Places in incremental folder to prevent cluttering main storage.
"""
def createDateTimePath():
    filePath     = mountPoint + savePath
    extension    = ".parquet"
    timeStamp    = createTimeStamp()
    dateTimePath = (filePath + timeStamp + extension)
    
    return dateTimePath
  
  
"""
Add time stamp to file path to prevent saving over orignial file
Places in incremental folder to prevent cluttering main storage

Parameters:

  -string_sourceType (--string): csv or parquet
"""
def writeParquetToStorage(string_sourceType):
    dateTimePath = createDateTimePath()
    
    if string_sourceType == 'parquet':
        parquetFile.write.parquet(dateTimePath)
    elif string_sourceType =='csv':
        csvFile.write.csv(dateTimePath)

# COMMAND ----------

# ReadCSV file in blob
csvFile = readAndShowCSV('csv/userdata1.csv')

# COMMAND ----------

# Write csv file back to blob as parquet
writeParquetToStorage('csv')

# COMMAND ----------

##Read back the parquet file we just converted from csv -> parquet & stored in blob
parquetFile = readIncrementalParquetFile('02-04-2020_18-53')

# COMMAND ----------

parquetFile.show(30)

# COMMAND ----------

parquetFile.describe().show()

# COMMAND ----------

# How many distinct registration dates are there? If more than 1, Filter by that.
parquetFile.select("registration_dttm").distinct().count()

# COMMAND ----------

parquetFile.filter(parquetFile.registration_dttm =="2016-02-03").count()

# COMMAND ----------

# Converts Dataframe into a list
from pyspark.sql.functions import to_timestamp

    #    df.select(to_timestamp(df.t                                                ).alias('dt')).collect()
    #    df.select(to_timestamp(df.t,                          'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
dates_as_list =  parquetFile.select(to_timestamp(parquetFile.registration_dttm, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
for elem in dates_as_list:
     print (elem)

# COMMAND ----------

parquetFile

# COMMAND ----------

import pyspark.sql.functions as func
from pyspark.sql import SQLContext
import datetime, time 

# NOTE: This data set all occurs on the same day, so you must filter by HOUR
start_date = "2016-02-03 00:00:00"
end_date   = "2016-02-03 23:59:59"

# Filtered dates
after_start_date  = parquetFile["registration_dttm"] >= start_date
before_end_date   = parquetFile["registration_dttm"] <= end_date
between_two_dates = after_start_date & before_end_date # returns a column

# COMMAND ----------

# Filter & return rows between the start & end date
df1 = parquetFile.filter(parquetFile["registration_dttm"] >= func.lit(start_date)) \
                 .filter(parquetFile["registration_dttm"] <= func.lit(end_date  )).show()