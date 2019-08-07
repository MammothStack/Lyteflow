# Third party imports
import pytest
import pandas as pd

# Local imports
from lyteflow.construct import PipeSystem
from lyteflow.kernels.split import *
from lyteflow.kernels.io import *
from lyteflow.kernels.merge import *
from lyteflow.kernels.filter import *


@pytest.fixture()
def df_small():
    return pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])


class TestColumnFilter:
    def test_transform_all_columns(self, df_small):
        expected = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
        cf = ColumnFilter(columns=["a", "b"])
        assert (expected == cf.transform(df_small)).all().all()

    def test_transform_some_columns(self, df_small):
        expected = pd.DataFrame([[1], [3]], columns=["a"])
        cf = ColumnFilter(columns=["a"])
        assert (expected == cf.transform(df_small)).all().all()

    def test_transform_no_columns(self, df_small):
        expected = pd.DataFrame(index=[0, 1])
        cf = ColumnFilter(columns=[])
        assert (expected == cf.transform(df_small)).all().all()

    def test_transform_wrong_columns_ignore(self, df_small):
        expected = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
        cf = ColumnFilter(columns=["a", "b", "c", "d"], ignore_absent=True)
        assert (expected == cf.transform(df_small)).all().all()

    def test_transform_wrong_columns_not_ignore(self, df_small):
        cf = ColumnFilter(columns=["a", "b", "c", "d"], ignore_absent=False)
        with pytest.raises(KeyError):
            cf.transform(df_small)
