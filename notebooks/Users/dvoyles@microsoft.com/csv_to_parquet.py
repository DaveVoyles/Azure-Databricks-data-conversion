# Databricks notebook source
# DBTITLE 1,Instructions
# AUTHOR: Dave Voyles | Microsoft | April-2020 | https://github.com/DaveVoyles/Azure-Databricks-data-conversion

# GOAL: Incremental loading of parquet files

#	1) Take something that has "date time" and run a filter that filters to one day at a time.
#   2) Then add that data to an existing parquet data set.

# Steps:

#  1) Mount to Azure blob storage -- allows us to read/write. Mount point is: "/mnt/blobmount"
#  2) Read .csv from from "/mnt/blobmount/example/data/users.csv"
#  3) Write .csv file back to blob as parquet OR csv (in this example, parquet) to "/example/data/users/incremental" folder.
#      NOTE: We are appending the current date, up to the minute, to prevent overwriting the existing parquet file
#  4) Read back parquet file as data frame
#  5) Filter df by between a start & end date
#      NOTE: All transactions occur on the same day, so we filter by HOUR here ('03') to give us fewer results
#  6) Append newly filtered DF to existing parquet file

# Conclusion

#  We have seen that it is very easy to add data to an existing Parquet file. This works very well when you’re adding data - as opposed to updating or deleting existing records - to a cold data store (Amazon S3, for instance). 
#  In the end, this provides a cheap replacement for using a database when all you need to do is offline analysis on your data.

#  You can add partitions to Parquet files, but you can’t edit the data in place.

#  SOURCE: https://stackoverflow.com/questions/28837456/updating-values-in-apache-parquet-file
#  SOURCE: http://aseigneurin.github.io/2017/03/14/incrementally-loaded-parquet-files.html

# COMMAND ----------

# Can name this whatever you like, but first folder MUST be '/mnt'
mountPoint = "/mnt/blobmount"

# Loading data from .csv source for this example
loadPath   = "/example/data/users.csv"

# Saving all data into the 'incremental' folder 
savePath   = "/example/data/users/incremental"

# .csv file we'll be reading from
sCSVPath = 'csv/userdata1.csv'

# We'll use this to append to the current parquet file we are working from
sParquetPath = "02-04-2020_18-53"

# Date we are going to filter the parquet file by
sRegistration_dttm = "2016-02-03"

# /mnt/blobmount/example/data/users/incremental/02-04-2020_18-53.parquet
parquetAppendPath = mountPoint + savePath + "/" + sParquetPath + ".parquet"

# COMMAND ----------

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
        csvFile.write.parquet(dateTimePath)
    elif string_sourceType =='csv':
        csvFile.write.csv(dateTimePath)

# COMMAND ----------

# ReadCSV file in blob
csvFile = readAndShowCSV(sCSVPath)

# COMMAND ----------

# Write csv file back to blob as parquet OR csv
writeParquetToStorage('parquet')

# COMMAND ----------

# Read back the parquet file we just converted from csv -> parquet & stored in blob
parquetFile = readIncrementalParquetFile(sParquetPath)

# COMMAND ----------

"""
Filters current dataframe to only return values between start & end dates

Parameters:
  -sEndHour (--string): 24-hour format. Which hour should we end on?  EX: "23" for 11pm
  
Returns:
     DataFrame filtered between the selected dates (hours)
"""
def FilterByDates(sEndHour):
  import pyspark.sql.functions as func
  from pyspark.sql import SQLContext
  import datetime, time 

  # NOTE: This data set all occurs on the same day, so you must filter by HOUR
  start_date = "2016-02-03           00:00:00"
  end_date   = "2016-02-03 "+sEndHour+":59:59"

  # Filtered dates
  after_start_date  = parquetFile["registration_dttm"] >= start_date
  before_end_date   = parquetFile["registration_dttm"] <= end_date
  between_two_dates = after_start_date & before_end_date # returns a column

  # Filter & return rows between the start & end date
  filteredDF = parquetFile.filter(parquetFile["registration_dttm"] >= func.lit(start_date)) \
                          .filter(parquetFile["registration_dttm"] <= func.lit(end_date  ))
  
  print(filteredDF)
  filteredDF.show()
  
  return filteredDF

# COMMAND ----------

dfFiltered = FilterByDates("03")

# COMMAND ----------

# Append mode means that when saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
# https://stackoverflow.com/questions/39234391/how-to-append-data-to-an-existing-parquet-file
dfFiltered.write.mode('append').parquet(parquetAppendPath)

# COMMAND ----------

# NOT USED. I wanted to conver the time stamps to a list, thinking it would be easier to filter. 
"""
Converts Dataframe into a list

Parameters:
  -df (--string): Dataframe to be converted
  
Returns:
     EX: Row(dt=datetime.datetime(2016, 2, 3, 7, 55, 29))
"""
def convertDfColToList(df):
  from pyspark.sql.functions import to_timestamp

      #   df.select(to_timestamp(df.t                                                ).alias('dt')).collect()
      #   df.select(to_timestamp(df.t,                          'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
  dates_as_list =  df.select(to_timestamp(df.registration_dttm, 'yyyy-MM-dd HH:mm:ss').alias('dt')).collect()
  for elem in dates_as_list:
       print (elem)
      
convertDfColToList(parquetFile)

# COMMAND ----------

# Trying to understand each column type to see if it was possible to add a new row. They all return type: string
parquetFile.describe()

# COMMAND ----------

# NOT USED. Add new row to data frame
# https://stackoverflow.com/questions/47556025/pyspark-add-new-row-to-dataframe/47556546
newRow = spark.createDataFrame([ 
  "2016-02-03 00:00:01",
  "1",
  "Dave",
  "Voyles",
  "davevoyles@mail.com",
  "Male",
  "1.197.201.2",
  "6759521864920116",
  "United States",
  "3/8/1900",
  "100000.00",
  "Programmer",
  "null",
  "test"
])
# TypeError: Can not infer schema for type: <class 'str'>
parquetFile.union(newRow)
parquetFile.show()