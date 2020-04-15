import unittest
from pytest_spark import spark_context
from pyspark import SparkContext, SparkConf
import csv_to_parquet

# Inherits from unittest.TestCase
# Gives us access to testing capibilities 
class TestParquet(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf().setMaster("local[*]").setAppName("testing")
        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()

    ## NOTE: How does this know when to use spark_context vs spark(Databricks)?
    def test_readAndShowCSV(spark_context):
        csvFile = csv_to_parquet.readAndShowCSV()

if __name__ == '__main__':
    unittest.main()

