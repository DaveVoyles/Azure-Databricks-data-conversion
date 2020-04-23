# Databricks notebook source
# Data sample acquired from: http://dailydoseofexcel.com/archives/2013/04/12/sample-fixed-width-text-file/

# Name | Default Val | Label
dbutils.widgets.text("input_path", "","") 
my_input = dbutils.widgets.get("input_path")
print(my_input)

dbutils.widgets.text("output_path", "","") 
my_output = dbutils.widgets.get("output_path")
print(my_output)

dbutils.widgets.text("filename", "","") 
dbutils.widgets.get("filename")

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