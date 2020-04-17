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

'''
Reads data from '/mnt/blobmount/example/data/users.csv' folder
TODO: Allow this to read parquet as well

Paramters:
  -sFilePath(--string): In this format: 'csv/userdata1.csv'
  
Returns: 
  pyspark.sql.dataframe.DataFrame
'''
def readAndShowCSV(sFilePath):
  csvFile = spark.read.csv(mountPoint+loadPath+'/' + sFilePath, header=True, inferSchema=True)
  csvFile.show(5)

  return csvFile


'''
After saving a .csv or .parquet file to the incremental folder, read it back.
Reads data from "/mnt/blobmount/example/data/users/incremental"

Parameters:
  -sDate (--string): In this format: '31-03-2020_19-08'
  
Returns:
  pyspark.sql.dataframe.DataFrame
'''
def readIncrementalParquetFile(sDate): 
  parquetFile = spark.read.parquet(mountPoint + savePath + '/' + sDate + '.parquet')
  parquetFile.show(5)
  
  return parquetFile


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


# ReadCSV file in blob
csvFile = readAndShowCSV(sCSVPath)

# Write csv file back to blob as parquet OR csv
writeParquetToStorage('parquet')

# Read back the parquet file we just converted from csv -> parquet & stored in blob
parquetFile = readIncrementalParquetFile(sParquetPath)

"""
Filters current dataframe to only return values between start & end dates

Parameters:
  -sEndHour (--string): 24-hour format. Which hour should we end on?  EX: "23" for 11pm
  
Returns:
     DataFrame filtered between the selected dates (hours)
"""
def FilterByDates(sEndHour = "03", _partquetFile = parquetFile):
  import pyspark.sql.functions as func
  from pyspark.sql import SQLContext
  import datetime, time 

  # NOTE: This data set all occurs on the same day, so you must filter by HOUR
  start_date = "2016-02-03           00:00:00"
  end_date   = "2016-02-03 "+sEndHour+":59:59"

  # Filtered dates
  after_start_date  = _parquetFile["registration_dttm"] >= start_date
  before_end_date   = _parquetFile["registration_dttm"] <= end_date
  between_two_dates = after_start_date & before_end_date # returns a column

  # Filter & return rows between the start & end date
  filteredDF = _parquetFile.filter(_parquetFile["registration_dttm"] >= func.lit(start_date)) \
                           .filter(_parquetFile["registration_dttm"] <= func.lit(end_date  ))
  
  print(filteredDF)
  filteredDF.show()
  
  return filteredDF


dfFiltered = FilterByDates("03")

# Append mode means that when saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data.
# https://stackoverflow.com/questions/39234391/how-to-append-data-to-an-existing-parquet-file
dfFiltered.write.mode('append').parquet(parquetAppendPath)
