# Third party imports
import pytest

# local imports
from lyteflow.construct import PipeSystem
from lyteflow.kernels import *
from lyteflow.util import *


@pytest.fixture()
def pipe_system_and_elements():
    in_1 = Inlet(convert=False, name="in_1")
    sca = Scaler(scalar=1 / 255)(in_1)
    rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
    out_1 = Outlet(name="out_1")(rot)

    in_2 = Inlet(convert=True, name="in_2")
    cat = Categorizer(sparse=True)(in_2)
    dup = Duplicator()(cat)
    con = Concatenator()(dup, dup, dup)
    out_2 = Outlet(name="out_2")(con)

    all_elements = [in_1, sca, rot, out_1, in_2, cat, dup, con, out_2]

    return (
        PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps"),
        all_elements,
    )


class TestFetchElements:
    def test_count_all(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert len(
            fetch_pipe_elements(ps, ignore_inlets=False, ignore_outlets=False)
        ) == len(elements)

    def test_count_ignore_inlets(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert (
            len(fetch_pipe_elements(ps, ignore_inlets=True, ignore_outlets=False)) == 7
        )

    def test_count_ignore_outlets(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert (
            len(fetch_pipe_elements(ps, ignore_inlets=False, ignore_outlets=True)) == 7
        )

    def test_count_ignore_inlets_outlets(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert (
            len(fetch_pipe_elements(ps, ignore_inlets=True, ignore_outlets=True)) == 5
        )

    def test_content_all(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert set(fetch_pipe_elements(ps)) == set(elements)

    def test_content_ignore_inlets(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert set(fetch_pipe_elements(ps, ignore_inlets=True)) == set(
            elements
        ).difference(ps.inlets)

    def test_content_ignore_outlets(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert set(fetch_pipe_elements(ps, ignore_outlets=True)) == set(
            elements
        ).difference(ps.outlets)

    def test_content_ignore_inlets_outlets(self, pipe_system_and_elements):
        ps, elements = pipe_system_and_elements
        assert set(
            fetch_pipe_elements(ps, ignore_outlets=True, ignore_inlets=True)
        ) == set(elements).difference(list(ps.inlets) + list(ps.outlets))

    def test_wrong_input(self):
        with pytest.raises(AttributeError):
            fetch_pipe_elements(3)


class TestColumnNamesToFormatted:
    def test_str(self):
        assert column_names_to_formatted_list("test") == [["test"]]

    def test_list_of_str(self):
        assert column_names_to_formatted_list(["test", "test"]) == [["test", "test"]]

    def test_list_of_list(self):
        assert column_names_to_formatted_list([["test", "test"]]) == [["test", "test"]]

    def test_mixed_list(self):
        assert column_names_to_formatted_list(["test", ["test"]]) == [
            ["test"],
            ["test"],
        ]

    def test_wrong_input(self):
        with pytest.raises(ValueError):
            assert column_names_to_formatted_list(4)

    def test_wrong_input_1(self):
        with pytest.raises(ValueError):
            assert column_names_to_formatted_list(None)
