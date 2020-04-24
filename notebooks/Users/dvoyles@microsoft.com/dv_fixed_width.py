# Databricks notebook source
# Instrument for unit tests. This is only executed in local unit tests, not in Databricks.
# More info here: https://github.com/microsoft/DataOps/tree/master/Python/packages/databricks-test
if 'dbutils' not in locals():
    import databricks_test
    databricks_test.inject_variables()

# COMMAND ----------

# Check if blob storage is already mounted. If not, mount.
# Using -- AZURE KEY VAULT -- here, by authenticating Databricks w/ Key vault once

# INSTRUCTIONS: https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
if mountPoint in [mp.mountPoint for mp in dbutils.fs.mounts()]:
    print(mountPoint + " exists!")
else:
  dbutils.fs.mount(
  source        = "wasbs://dv-hdinsight-2020-03-30t16-29-59-717z@dvhdinsighthdistorage.blob.core.windows.net",
  mount_point   = mountPoint,
  extra_configs = {"fs.azure.account.key.dvhdinsighthdistorage.blob.core.windows.net":dbutils.secrets.get(scope = "dv-db-blob-scope-name", key = "dv-db-blob-secret")})

# COMMAND ----------

# Data sample acquired from: http://dailydoseofexcel.com/archives/2013/04/12/sample-fixed-width-text-file/

# Name | Default Val | Label
dbutils.widgets.text("blob_input", "","") 
my_input = dbutils.widgets.get("blob_input")
print(my_input)

dbutils.widgets.text("blob_output", "","") 
my_output = dbutils.widgets.get("blob_output")
print(my_output)

dbutils.widgets.text("filename", "","") 
dbutils.widgets.get("filename")

# COMMAND ----------

df = spark.read.text(my_input).show()
df(print)

# COMMAND ----------

# Read fixed-width: https://stackoverflow.com/questions/41944689/pyspark-parse-fixed-width-text-file

"""
df = spark.read.text("/tmp/sample.txt")
df.select(
    df.value.substr(1,3).alias('id'),
    df.value.substr(4,8).alias('date'),
    df.value.substr(12,3).alias('string'),
    df.value.substr(15,4).cast('integer').alias('integer')
).show()


+---+--------+------+-------+
| id|    date|string|integer|
+---+--------+------+-------+
|001|01292017|   you|   1234|
|002|01302017|    me|   5678|
+---+--------+------+-------+
+++

# COMMAND ----------

## May have to store data in here: https://www.element61.be/en/resource/how-can-we-use-azure-databricks-and-azure-data-factory-train-our-ml-algorithms