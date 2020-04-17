import unittest
import pandas as pd
import logging
import csv_to_parquet 
from io             import BytesIO
from unittest.mock  import patch
from pyspark.sql import SparkSession

# Inherits from unittest.TestCase, gives us access to testing capibilities 
class TestParquet(unittest.TestCase):
 
    #  ######## CREATE LOCAL PYSPARK ENV FOR TESTING ########
    # @classmethod
    # def suppress_py4j_logging(cls):
    #     logger = logging.getLogger('py4j')
    #     logger.setLevel(logging.WARN)

    # @classmethod
    # def create_testing_pyspark_session(cls):
    #     return (SparkSession.builder
    #     .master('local[2]')
    #     .appName('my-local-testing-pyspark-context')
    #     .enableHiveSupport()
    #     .getOrCreate())

    # ######## SETUP AND TEAR DOWN ########
    # @classmethod
    # def setUpClass(cls):
    #     cls.suppress_py4j_logging()
    #     cls.spark = cls.create_testing_pyspark_session()

    # @classmethod
    # def tearDownClass(cls):
    #     cls.spark.stop()

     ######## TESTED METHODS ########
    def test_readAndShowCSV(self):
        with databricks_test.session() as dbrickstest:
          # csvFile = readAndShowCSV(test_data_file_path)
          # Read CSV 
          # myCSV = pd.read_csv(test_data_file_path)
          # print(myCSV)

          from pandas.testing import assert_frame_equal

          with TemporaryDirectory() as tmp_dir:
            input_dir = '../../../data/userdata1.csv'
            out_dir   = f"{tmp_dir}/out"

            # Hard-coded data from first row of .csv
            df_data = ["2016-02-03T07:55:29Z", 
            "1",
            "Amanda",
            "Jordan",
            "ajordan0@com.com",
            "Female", 
            "1.197.201.2",
            "6759521864920116", 
            "Indonesia",
            "3/8/1971", 
            "49756.53",
            "Internal Auditor", 
            "1E+02"
            ]

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

            df_data_filter_by_dates = [
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

                   
            # Provide input and output location as widgets to notebook
            switch = {
                "input": test_data_file_path,
                "output": out_dir,
            }

            # Run notebook
            dbrickstest.run_notebook(".", "csv_to_parquet") 

            # Notebook produces a Parquet file (directory)
            resultDF = pd.read_parquet(out_dir)
                
            # Local hard-coded data vs .csv we are reading in
            expected_Output       = pd.DataFrame(data, df_columns)
            real_Output           = FilterByDates()
            real_Output_First_Row = real_Output.iloc[0]

        pd.testing.assert_frame_equal(
          expected_output,
          real_Output_First_Row,
          check_like=True,
        )

        # # Compare first column of csv to our hard-coded column & assert equal
        # expectedDF = pd.read_csv("tests/etl_expected.csv")
        # assert_frame_equal(expectedDF, resultDF, check_dtype=False)

# Will run all of our tests
if __name__ == '__main__':
    unittest.main()

