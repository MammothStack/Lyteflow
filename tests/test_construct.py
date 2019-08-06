# Third party imports
import pytest
import numpy as np

# local imports
from lyteflow.construct import PipeSystem
from lyteflow.kernels import *


@pytest.fixture()
def images():
    return np.random.randint(1, 5, (10, 10, 10))


@pytest.fixture()
def labels():
    return np.random.randint(0, 10, 10)


@pytest.fixture()
def basic_pipesystem():
    in_1 = Inlet(convert=False, name="in_1")
    out_1 = Outlet(name="out_1")(in_1)

    in_2 = Inlet(convert=True, name="in_2")
    out_2 = Outlet(name="out_2")(in_2)

    return PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps")


@pytest.fixture()
def complex_pipesystem():
    in_1 = Inlet(convert=False, name="in_1")
    sca = Scaler(scalar=1 / 255)(in_1)
    rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
    out_1 = Outlet(name="out_1")(rot)

    in_2 = Inlet(convert=True, name="in_2")
    cat = Categorizer(sparse=True)(in_2)
    dup = Duplicator()(cat)
    con = Concatenator()(dup, dup, dup)
    out_2 = Outlet(name="out_2")(con)

    return PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps")


@pytest.fixture()
def basic_pipesystem_json():
    return PipeSystem.from_json(json_file_name="tests/ps_simple.json")


@pytest.fixture()
def complex_pipesystem_json():
    return PipeSystem.from_json(json_file_name="tests/ps_complex.json")


class TestPipeSystemConfig(object):
    def test_keys(self, basic_pipesystem):
        expected = {"outlets", "inlets", "elements", "name"}
        assert set(basic_pipesystem.to_config().keys()) == expected

    def test_contents_name(self, basic_pipesystem):
        assert "ps" == basic_pipesystem.to_config()["name"]

    def test_contents_inlet_1(self, basic_pipesystem):
        attributes = basic_pipesystem.to_config()["inlets"][0]["attributes"]
        attributes.pop("id")
        assert attributes == {"name": "in_1", "convert": False}

    def test_contents_inlet_2(self, basic_pipesystem):
        attributes = basic_pipesystem.to_config()["inlets"][1]["attributes"]
        attributes.pop("id")
        assert attributes == {"name": "in_2", "convert": True}

    def test_contents_outlet_1(self, basic_pipesystem):
        attributes = basic_pipesystem.to_config()["outlets"][0]["attributes"]
        attributes.pop("id")
        assert attributes == {"name": "out_1"}

    def test_contents_outlet_2(self, basic_pipesystem):
        attributes = basic_pipesystem.to_config()["outlets"][1]["attributes"]
        attributes.pop("id")
        assert attributes == {"name": "out_2"}

    def test_to_from_json(
        self, basic_pipesystem, basic_pipesystem_json, images, labels
    ):
        res = basic_pipesystem.flow(images, labels)
        res_json = basic_pipesystem_json.flow(images, labels)

        assert (res[0] == res_json[0]).all() and ((res[1] == res_json[1]).all().all())
