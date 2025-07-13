import unittest

import numpy as np
import pandas as pd

from pandas.testing import assert_series_equal
from pandas.testing import assert_frame_equal
from pandas import Timestamp
from src.cleaner.DataCleanerImpl import DataCleanerImpl


class DataCleanerImplTest(unittest.TestCase):

    def test_clean_string_one_column(self):
        data = {
            'title': [' A', 'a', ' b', 'B '],
            'value': [10, 20, 30, np.nan]
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_string(df, ["title"])

        assert_series_equal(df.title, pd.Series(['A', 'A', 'B', 'B'], name="title"))

    def test_clean_string_multiple_column(self):
        data = {
            'title1': [' A', 'a', ' b', 'B '],
            'title2': [' Test', 'this ', np.nan, 'a Test '],
            'title3': [' A2', 'a', '1 b', 'B 3 ']
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_string(df, ["title1", "title2", "title3"])

        val_data = {
            'title1': ['A', 'A', 'B', 'B'],
            'title2': ['TEST', 'THIS', np.nan, 'A TEST'],
            'title3': ['A2', 'A', '1 B', 'B 3']
        }

        assert_frame_equal(df, pd.DataFrame(val_data))

    def test_clean_numbers_one_column(self):
        data = {
            'value1': ["10.4", "20.4", 30, np.nan]
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_numbers(df, ["value1"])

        val_data = {
            'value1': [10.4, 20.4, 30, np.nan]
        }

        assert_frame_equal(df, pd.DataFrame(val_data))


    def test_clean_numbers_multiple_column(self):
        data = {
            'value1': ["10.4", 20.4, "30", np.nan],
            'value2': ["10.3", "20", 30, np.nan]
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_numbers(df, ["value1", "value2"])
        val_data = {
            'value1': [10.4, 20.4, 30, np.nan],
            'value2': [10.3, 20, 30, np.nan]
        }

        assert_frame_equal(df, pd.DataFrame(val_data))

    def test_clean_date_one_column(self):
        data = {
            "value1": ["1 January 1970", "2020/03/04", "02/05/2023"],
            "value2": ["3 February 1976", "2023/06/14", "02/05/2023"]
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_date(df, ["value1"])

        val_data = {
            "value1": [Timestamp(day=1, month=1, year=1970), Timestamp(day=4,month=3, year=2020), Timestamp(day=2, month=5, year=2023)],
            "value2": [Timestamp(day=3, month=1, year=1976), Timestamp(day=14, month=6, year=2023), Timestamp(day=2, month=5, year=2023)]
        }
        val_df = pd.DataFrame(val_data)

        assert_series_equal(df.value1, val_df.value1)

    def test_clean_date_multiple_column(self):
        data = {
            "value1": ["1 January 1970", "2020/03/04", "02/05/2023"],
            "value2": ["3 February 1976", "2023/06/14", "02/05/2023"]
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_date(df, ["value1", "value2"])

        val_data = {
            "value1": [Timestamp(day=1, month=1, year=1970), Timestamp(day=4, month=3, year=2020),
                       Timestamp(day=2, month=5, year=2023)],
            "value2": [Timestamp(day=3, month=2, year=1976), Timestamp(day=14, month=6, year=2023),
                       Timestamp(day=2, month=5, year=2023)]
        }
        val_df = pd.DataFrame(val_data)

        assert_frame_equal(df, val_df)



