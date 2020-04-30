# Databricks notebook source
# Data sample acquired from: http://dailydoseofexcel.com/archives/2013/04/12/sample-fixed-width-text-file/
# Parameters we are passing in and/or returning from this notebook

# NAME| DEFAULT VALUE | LABEL

dbutils.widgets.text("input", "","") 
dbutils.widgets.get("input")

dbutils.widgets.text("output", "","") 
dbutils.widgets.get("output")

dbutils.widgets.text("filename", "","") 
dbutils.widgets.get("filename")

dbutils.widgets.text("pipelineRunId", "","") 
dbutils.widgets.get("pipelineRunId")

# COMMAND ----------

# Unmount blob storage if it currently exists. 
# Without this, Databricks will throw an error each time you try to load data from storage
def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

sub_unmount('/mnt/adfdata')

# Mount blob storage to Databricks
storageName = "dvhdinsighthdistorage"
try:
    dbutils.fs.mount(
    source        = "wasbs://sinkdata@"+storageName+".blob.core.windows.net",
    mount_point   = "/mnt/adfdata",
    extra_configs = {"fs.azure.account.key."+storageName+".blob.core.windows.net":dbutils.secrets.get(scope = "dv-db-blob-scope-name",key = "dv-db-blob-secret")})
    
    print('mounted sucessfully')  
except Exception as e:
  # The error message has a long stack trace.  This code tries to print just the relevent line indicating what failed.
  import re
  result = re.findall(r"^\s*Caused by:\s*\S+:\s*(.*)$", e.message, flags=re.MULTILINE)
  if result:
    print (result[-1]) # Print only the relevant error message
  else:
    print (e) # Otherwise print the whole stack trace.

# COMMAND ----------

# MAGIC %fs ls /mnt/adfdata/

# COMMAND ----------

# Read and display text file from blob storage
from pyspark.sql.functions import desc

inputFile = "dbfs:/mnt/adfdata"+getArgument("input")+"/"+getArgument("filename")
print(inputFile)

initialDF = (spark.read           # The DataFrameReader
  .option("header", "true")       # Use first line of all files as header
  .option("inferSchema", "true")  # Automatically infer data types
  .text(inputFile)                # Creates a DataFrame from Text after reading in the file
)
display(initialDF)

# COMMAND ----------

print(inputFile)

# COMMAND ----------

# TODO: Filtering here

# COMMAND ----------

#Removing Extension from filename
import os
file = os.path.splitext(getArgument("filename"))[0]
print(file)

# COMMAND ----------

# write the output into parquet
initialDF.write.mode("overwrite").parquet("dbfs:/mnt/adfdata"+getArgument("output")+"/"+file+"_"+getArgument("pipelineRunId")+"/parquet") #for parquet