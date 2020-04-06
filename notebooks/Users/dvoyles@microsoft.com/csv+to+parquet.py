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

# Converts Dataframe into a list (to_timestamp)
from pyspark.sql.functions import to_timestamp

    #    df.select(to_timestamp(df.t                                                ).alias('dt')).collect()
    #    df.select(to_timestamp(df.t,                          'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
to_timestamp =  parquetFile.select(to_timestamp(parquetFile.registration_dttm, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
to_timestamp

# COMMAND ----------

parquetFile

# COMMAND ----------

# Beginning & end dates
start_date = "2016-02-03"
end_date   = "2016-02-04"

# Filtered dates
after_start_date  = parquetFile["registration_dttm"] >= start_date
before_end_date   = parquetFile["registration_dttm"] <= end_date
between_two_dates = after_start_date & before_end_date

import pyspark.sql.functions as func
from pyspark.sql import SQLContext
import datetime, time 

# COMMAND ----------

parquetFilecopy = parquetFile

# COMMAND ----------

to_timestamp.withColumn("hour",   hour  (col("parquetFilecopy"))).withColumn("minute", minute(col("parquetFilecopy"))).withColumn("second", second(col("parquetFilecopy"))).show(false)

# COMMAND ----------

# parquetFile.filter(parquetFile.registration_dttm.between( ("2016-02-03"))


#todate = parquetFile.select(func.to_date(parquetFile.registration_dttm).alias("time"))
#sf = todate.filter(todate.time > date_from).filter(todate.time < date_to)

dates = ("2016-02-03",  "2016-02-04")
date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]

parquetFile.where((sf.my_col > date_from) & (sf.my_col < date_to))