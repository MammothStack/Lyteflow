# Third party imports
import pytest
import warnings

# Local imports
from lyteflow.construct import PipeSystem
from lyteflow.kernels.split import *
from lyteflow.kernels.io import *
from lyteflow.kernels.merge import *


@pytest.fixture()
def dupe_pipe():
    in_1 = Inlet(convert=True, name="in_2")
    x = Duplicator(n_result=3)(in_1)
    x = Concatenator(axis=1)(x)
    out_1 = Outlet(name="out_2")(x)
    return PipeSystem(inlets=[in_1], outlets=[out_1], name="ps")


@pytest.fixture()
def dupe_pipe_invalid():
    in_1 = Inlet(convert=True, name="in_2")
    x = Duplicator(n_result=3)(in_1)
    x = Concatenator(axis=1)(x, x)
    out_1 = Outlet(name="out_2")(x)
    return PipeSystem(inlets=[in_1], outlets=[out_1])


@pytest.fixture()
def simple_data_frame():
    return pd.DataFrame([[1, 2], [3, 4], [5, 6]], columns=["a", "b"])


@pytest.fixture()
def data_frame():
    return pd.DataFrame([[1, 2, 3, 4], [1, 2, 3, 4]], columns=["a", "b", "c", "d"])


class TestDuplicator:
    def test_transform(self, simple_data_frame):
        expected = pd.DataFrame([[1, 2], [3, 4], [5, 6]], columns=["a", "b"])
        res = Duplicator(n_result=2, name="dupe").transform(simple_data_frame)
        assert all([(i == expected).all().all() for i in res])

    def test_init_invalid_n_result(self):
        with pytest.raises(ValueError):
            Duplicator(n_result=0, name="dupe")

    def test_init_valid_n_result(self):
        Duplicator(n_result=None, name="dupe")

    def test_init_invalid_n_result_1(self):
        with pytest.raises(TypeError):
            Duplicator(n_result="a", name="dupe")

    def test_valid_setup(self, dupe_pipe, simple_data_frame):
        expected = pd.DataFrame(
            [[1, 2, 1, 2, 1, 2], [3, 4, 3, 4, 3, 4], [5, 6, 5, 6, 5, 6]],
            columns=["a", "b", "a", "b", "a", "b"],
        )
        assert (expected == dupe_pipe.flow(simple_data_frame)[0]).all().all()

    def test_invalid_setup(self, dupe_pipe_invalid, simple_data_frame):
        with pytest.warns(UserWarning):
            dupe_pipe_invalid.flow(simple_data_frame)

    def test_invalid_setup_1(self, dupe_pipe_invalid, simple_data_frame):
        expected = pd.DataFrame(
            [[1, 2, 1, 2], [3, 4, 3, 4], [5, 6, 5, 6]], columns=["a", "b", "a", "b"]
        )
        assert (expected == dupe_pipe_invalid.flow(simple_data_frame)[0]).all().all()


class TestColumnSplitter:
    def test_standard_split(self, simple_data_frame):
        cs = ColumnSplitter(columns=["a"])
        expected = pd.DataFrame([[1], [3], [5]], columns=["a"])
        assert (expected == cs.transform(simple_data_frame)[0]).all().all()

    def test_standard_split_split_rest(self, simple_data_frame):
        cs = ColumnSplitter(columns=["a"], split_rest=True)
        expected_a = pd.DataFrame([[1], [3], [5]], columns=["a"])
        expected_b = pd.DataFrame([[2], [4], [6]], columns=["b"])
        tr = cs.transform(simple_data_frame)
        assert (expected_a == tr[0]).all().all() and (expected_b == tr[1]).all().all()

    def test_split_non_existent_columns(self, data_frame):
        cs = ColumnSplitter(columns=[["a", "b"], ["d", "f"]], split_rest=True)
        expected_a = pd.DataFrame([[1, 2], [1, 2]], columns=["a", "b"])
        expected_b = pd.DataFrame([[4], [4]], columns=["d"])
        tr = cs.transform(data_frame)
        assert (expected_a == tr[0]).all().all() and (expected_b == tr[1]).all().all()

    def test_split_no_columns(self, data_frame):
        cs = ColumnSplitter(["e"])
        expected = pd.DataFrame(index=[0, 1])
        assert (expected == cs.transform(data_frame)[0]).all().all()
