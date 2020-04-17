import unittest
import pandas as pd
import logging
from csv_to_parquet_CLEAN import FilterByDates
from io             import BytesIO
from unittest.mock  import patch
from pyspark.sql import SparkSession
###################
from pyspark import SparkContext
from pyspark.sql import SQLContext

# Inherits from unittest.TestCase, gives us access to testing capibilities 
class TestParquet(unittest.TestCase):

  # BUG: Need to figure out how to pass this value in. 
  def test_FilterByDates():
    # Spark Context initialisation
    spark_context = SparkContext()
    sql_context   = SQLContext(spark_context)

    # Parquet data
    sParquetPath = "../../../data/02-04-2020_18-53.parquet"

    # Same CSV & Parquet files Databricks would read
    input_file = pd.read_parquet(sParquetPat)

    df_columns = columns =[
      "registration_dttm",
      "id",
      "first_name",
      "last_name",
      "email,gender",
      "ip_address",
      "cc",
      "country",
      "birthdate",
      "salary",
      "title",
      "comments"
    ]

    # First row of expected data
    expected_Output_First_Row = [
      "2016-02-03 01:09:31",
      "3",    
      "Evelyn",
      "Morgan",
      "emorgan2@altervis...",
      "Female", 
      "7.161.136.94",
      "6767119071901597",
      "Russia" ,
      "2/1/1960",
      "144972.51",
      "Structural Engineer",
      "null"
    ] 

    # First row of data from python file
    actual_Output           = FilterByDates()
    actual_Output_First_Row = actual_Output.iloc[0]
  
    # Compare expected vs actual
    pd.testing.assert_frame_equal(
      expected_Output_First_Row, 
      actual_Output_First_Row,
      check_like=True,
    )

    # Close the Spark Context
    spark_context.stop()
  
# Will run all of our tests
if __name__ == '__main__':
    unittest.main()