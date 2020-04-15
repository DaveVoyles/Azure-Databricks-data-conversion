import unittest
import pandas as pd
import logging
from csv_to_parquet import readAndShowCSV
from io             import BytesIO
from unittest.mock  import patch
from pyspark.sql import SparkSession

# Inherits from unittest.TestCase, gives us access to testing capibilities 
class TestParquet(unittest.TestCase):
 
     ######## CREATE LOCAL PYSPARK ENV FOR TESTING ########
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
        .master('local[2]')
        .appName('my-local-testing-pyspark-context')
        .enableHiveSupport()
        .getOrCreate())

    ######## SETUP AND TEAR DOWN ########
    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

     ######## TESTED METHODS ########
    def test_readAndShowCSV(self):
        # csvFile = readAndShowCSV()
           # test data file path, the fils is a csv file.
        test_data_file_path = '../../../data/userdata1.csv'

        # Read CSV 
        myCSV = pd.read_csv(test_data_file_path)
        print(myCSV)

        # Compare first column of csv to our hard-coded column & assert equal

# Will run all of our tests
if __name__ == '__main__':
    unittest.main()

