import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import os
import databricks.koalas as ks

from training.train import (
    train_forecasting_models,
    save_to_blob,
    load_data_from_blob,
)

class TestTraining(unittest.TestCase):
    def setUp(cls):
        cls.data = pd.DataFrame(
            {
                "store_id": ["100"] * 200,
                "product_id": ["10"] * 200,
                "date": [
                    str(d.date()) for d in pd.date_range("2017-03-01", periods=200)
                ],
                "net_prc": [1.99, 2.10] * 100,
                "retail_prc": [2.19, 2.10] * 100,
                "on_ad": [0] * 200,
                "on_front_page": [0] * 200,
                "on_display": [0] * 200,
                "on_tpr": [1, 0] * 100,
                "quantity": [0, 1, 2, 3, 4] * 40,
            }
        )
        cls.wrong_data = pd.DataFrame(
            {"col1": [9, 8, 7], "col2": [1, 2, 3], "target": [2, 4, 1]}
        )
        cls.y_col = "quantity"
        cls.x_cols = [
            "store_id",
            "product_id",
            "date",
            "net_prc",
            "retail_prc",
            "on_ad",
            "on_front_page",
            "on_display",
            "on_tpr",
        ]
        cls.run = "dummy_run"
        cls.file_path = "dummy_fpath"

        cls.models = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "product_id": [1, 2, 3],
                "create_date": ["2017-07-10", "2017-07-10", "2017-07-10"],
                "first_train_date": ["2017-07-10", "2017-07-10", "2017-07-10"],
                "last_train_date": ["2017-07-10", "2017-07-10", "2017-07-10"],
                "model": [1, 2, 3],
                "wape": [1, 2, 3],
                "bias": [1, 2, 3],
                "version": [1, 2, 3],
                "correlation_id": [1, 2, 3],
                "store_id": [1, 2, 3],
            }
        )

    @patch("training.train.save_to_blob")
    @patch("training.train.load_data_from_blob")
    @patch("training.train.appinsights", MagicMock())
    @patch("training.train.logger", MagicMock())
    @patch("training.train.spark", MagicMock())
    @patch.dict(
        os.environ,
        {
            "INPUT_STORAGE_PATH_DMP_CURATED": "fake",
            "INPUT_STORAGE_CONTAINER_DMP_CURATED": "fake",
            "INPUT_STORAGE_ACCOUNT_DMP_CURATED": "fake",
            "INPUT_STORAGE_ACCOUNT_DMP_KEY": "fake",
            "DIVISION": "fake",
            "OUTPUT_CORRELATION_ID": "fake",
        },
    )
    def test_train(self, mock_data, mock_save):
        # want to return a koalas version of the pandas test data to run through fake testing
        mock_data.return_value = ks.from_pandas(self.data)
        mock_save.return_value = "IM_A_MODEL!"

        # spark = configure_local_spark("account", "key")
        train_forecasting_models()

        # poke around the mocked save to see if we got a model
        mock_save.assert_called_once()
        # get the df we were gonna save and check for one model
        model_df = mock_save.call_args[0][1].to_pandas()
        self.assertEqual(len(model_df), 1)
        self.assertGreater(model_df.wape.iloc[0], 0)
        self.assertListEqual(
            list(model_df.columns),
            [
                "id",
                "store_id",
                "product_id",
                "create_date",
                "first_train_date",
                "last_train_date",
                "model",
                "wape",
                "bias",
                "version",
                "correlation_id",
            ],
        )

    @patch("training.train.load_data_from_blob")
    @patch("training.train.appinsights", MagicMock())
    @patch("training.train.logger", MagicMock())
    @patch("training.train.spark", MagicMock())
    def test_train_incorrect_columns(self, mock_data):
        mock_data.return_value = self.wrong_data
        with self.assertRaises(KeyError):
            train_forecasting_models()

    @patch("training.train.load_data_from_blob")
    @patch("training.train.appinsights", MagicMock())
    @patch("training.train.logger", MagicMock())
    @patch("training.train.spark", MagicMock())
    def test_train_no_target_column(self, mock_data):
        data = self.data[self.x_cols]
        mock_data.return_value = data
        with self.assertRaises(KeyError):
            train_forecasting_models()


    @patch("training.train.load_data_from_blob")
    @patch("training.train.appinsights", MagicMock())
    @patch("training.train.logger", MagicMock())
    @patch("training.train.spark", MagicMock())
    @patch.dict(
        os.environ,
        {
            "INPUT_STORAGE_PATH_DMP_CURATED": "fake",
            "INPUT_STORAGE_CONTAINER_DMP_CURATED": "fake",
            "INPUT_STORAGE_ACCOUNT_DMP_CURATED": "fake",
            "DIVISION": "fake",
        },
    )
    def test_train_empty_df_none(self, mock_data):
        data = None
        mock_data.return_value = data
        with self.assertRaises(TypeError):
            train_forecasting_models()

    @patch("training.train.load_data_from_blob")
    @patch("training.train.appinsights", MagicMock())
    @patch("training.train.logger", MagicMock())
    @patch.dict(
        os.environ,
        {
            "INPUT_STORAGE_PATH_DMP_CURATED": "fake",
            "INPUT_STORAGE_CONTAINER_DMP_CURATED": "fake",
            "INPUT_STORAGE_ACCOUNT_DMP_CURATED": "fake",
            "DIVISION": "fake",
        },
    )
    def test_train_empty_df(self, mock_data):
        data = pd.DataFrame()
        mock_data.return_value = data
        with self.assertRaises(ValueError):
            train_forecasting_models()


if __name__ == "__main__":
    unittest.main()
