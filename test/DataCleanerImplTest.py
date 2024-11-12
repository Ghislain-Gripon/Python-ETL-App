import unittest

import numpy as np
import pandas as pd

from src.DataCleanerImpl import DataCleanerImpl


class DataCleanerImplTest(unittest.TestCase):

    def test_clean_string(self):
        data = {
            'id': [1, 1, 2, 2],
            'title': [' A', 'a', ' b', 'B '],
            'value': [10, 20, 30, np.nan]
        }
        df = pd.DataFrame(data)
        df = DataCleanerImpl().clean_string(df, ["title"])

