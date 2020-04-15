import unittest
import pandas as pd
# from csv_to_parquet import readAndShowCSV
from io             import BytesIO
from unittest.mock  import patch

# Inherits from unittest.TestCase
# Gives us access to testing capibilities 
class TestParquet(unittest.TestCase):

    # def setUpClass(cls):
    #     conf = pyspark.SparkConf().setMaster("local[*]").setAppName("testing")
    #     cls.sc = pyspark.SparkContext(conf=conf)
    #     cls.spark = pyspark.SQLContext(cls.sc)

    # def tearDownClass(cls):
    #     cls.sc.stop()

    def test_readAndShowCSV(self):
        # csvFile = readAndShowCSV()

        # Read CSV 
        myCSV = pd.read_csv('../../../data/userdata1.csv')
        print(myCSV)

        # Compare first column of csv to our hard-coded column & assert equal


# Will run all of our tests
if __name__ == '__main__':
    unittest.main()

