# Databricks notebook source
# Instrument for unit tests. This is only executed in local unit tests, not in Databricks.
# More info here: https://github.com/microsoft/DataOps/tree/master/Python/packages/databricks-test
if 'dbutils' not in locals():
    import databricks_test
    databricks_test.inject_variables()

# COMMAND ----------

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

# Supply storageName and accessKey values
# You can also use Azure Key Vault to abstract this by connecting Key Vault -> Databricks
storageName = "dvhdinsighthdistorage"
accessKey    = "tb7o3VJklVaQ56nw6uqvZAFGfpx89QuXO7JeYntHoN3Mf5Tp7x7k30rHr00SiSGeNkhkr80bvRHdfzUqttzfTQ=="

# Unmount blob storage if it currently exists. 
# Without this, Databricks will throw an error each time you try to load data from storage
def sub_unmount(str_path):
    if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(str_path)

sub_unmount('/mnt/adfdata')

# Mount blob storage to Databricks
try:
  dbutils.fs.mount(
    source        = "wasbs://sinkdata@"+storageName+".blob.core.windows.net/",
    mount_point   = "/mnt/adfdata",
    extra_configs = {"fs.azure.account.key."+storageName+".blob.core.windows.net":
                     accessKey})
except Exception as e:
  # The error message has a long stack trace.  This code tries to print just the relevent line indicating what failed.
  import re
  result = re.findall(r"^\s*Caused by:\s*\S+:\s*(.*)$", e.message, flags=re.MULTILINE)
  if result:
    print (result[-1]) # Print only the relevant error message
  else:
    print (e) # Otherwise print the whole stack trace.

# COMMAND ----------

# MAGIC %fs ls /mnt/adfdata

# COMMAND ----------

from pyspark.sql.functions import desc

inputFile = "dbfs:/mnt/adfdata"+getArgument("input")+"/"+getArgument("filename")
initialDF = (spark.read           # The DataFrameReader
  .option("header", "true")       # Use first line of all files as header
  .option("inferSchema", "true")  # Automatically infer data types
  .text(fixed_width_path)         # Creates a DataFrame from Text after reading in the file
)
display(initialDF)

# COMMAND ----------

#Removing Extension from filename
import os
file = os.path.splitext(getArgument("filename"))[0]
print(file)

# COMMAND ----------

# write the output into parquet
initialDF.write.mode("overwrite").parquet("dbfs:/mnt/adfdata"+getArgument("output")+"/"+file+"_"+getArgument("pipelineRunId")+"/parquet") #for parquet