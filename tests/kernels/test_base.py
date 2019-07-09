# Third party imports
import pytest
import numpy as np

# Local imports
from lyteflow.kernels import *


@pytest.fixture()
def connected_pipe_elements():
    in_1 = Inlet(name="in")
    rot = Rotator([90, -90], remove_padding=True, keep_original=True, name="rot")(in_1)
    out_1 = Outlet(name="out")(rot)
    return in_1, rot, out_1


class TestConfig:
    def test_attributes_0(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        expected = {
            "name": "rot",
            "rotations": [-90, 0, 90],
            "remove_padding": True,
            "keep_original": True,
        }

        result = r.to_config()["attributes"]
        result.pop("id")
        assert expected == result

    def test_upstream_id(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        assert i.id == r.to_config()["upstream"][0]

    def test_downstream_id(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        assert o.id == r.to_config()["downstream"][0]

    def test_to_and_from_config_upstream(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), upstream=i, downstream=o)
        assert c_pe.upstream.id == i.id

    def test_to_and_from_config_downstream(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), upstream=i, downstream=o)
        assert c_pe.downstream.id == o.id
