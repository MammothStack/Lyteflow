# Third party imports
import pytest
import numpy as np
import pandas as pd

# Local imports
from lyteflow.kernels import *


@pytest.fixture()
def random_flow_data():
    return FlowData(
        data=pd.DataFrame(
            np.random.randint(0, 2, (10, 5)), columns=["a", "b", "c", "d", "e"]
        )
    )


@pytest.fixture()
def connected_pipe_elements():
    in_1 = Inlet(name="in")
    rot = Rotator([90, -90], remove_padding=True, keep_original=True, name="rot")(in_1)
    out_1 = Outlet(name="out")(rot)
    return in_1, rot, out_1


@pytest.fixture()
def base_pipe_element():
    in_1 = Inlet(name="in")
    pe = PipeElement(name="pe")(in_1)
    Outlet(name="out")(pe)
    return pe


@pytest.fixture()
def connected_pipe_elements_2():
    in_1 = Inlet(name="in")
    rot = Rotator([90, -90], remove_padding=True, keep_original=True, name="rot")(in_1)
    out_1 = Outlet(name="out")(rot)

    in_2 = Inlet(name="in_2", convert=True)
    out_2 = Outlet(name="out_2")(in_2)
    return rot


@pytest.fixture()
def connected_pipe_elements_2_requirements():
    in_1 = Inlet(name="in")
    rot = Rotator([90, -90], remove_padding=True, keep_original=True, name="rot")(in_1)
    out_1 = Outlet(name="out")(rot)

    in_2 = Inlet(name="in_2", convert=True)
    out_2 = Outlet(name="out_2")(in_2)

    rot.add_requirement(Requirement(in_2, "convert", "remove_padding"))
    return rot


class TestConfig:
    def test_attributes_0(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        expected = {
            "name": "rot",
            "rotations": [-90, 0, 90],
            "remove_padding": True,
            "keep_original": True,
            "n_output": 3,
        }

        result = r.to_config()["attributes"]
        result.pop("id")
        assert expected == result

    def test_upstream_id(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        assert i.id == r.to_config()["unconfigured_upstream"][0]

    def test_downstream_id(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        assert o.id == r.to_config()["unconfigured_downstream"][0]

    def test_to_and_from_config_upstream(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), element_id=True)
        assert c_pe._unconfigured_upstream[0] == i.id

    def test_to_and_from_config_downstream(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), element_id=True)
        assert c_pe._unconfigured_downstream[0] == o.id

    def test_to_and_from_config_upstream_reconfigure(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), element_id=True)
        c_pe.reconfigure(*connected_pipe_elements)
        assert c_pe.upstream[0] == i

    def test_to_and_from_config_downstream_reconfigure(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), element_id=True)
        c_pe.reconfigure(*connected_pipe_elements)
        assert c_pe.downstream[0] == o

    def test_reconfigure_incomplete_elements(self, connected_pipe_elements):
        i, r, o = connected_pipe_elements
        c_pe = PipeElement.from_config(r.to_config(), element_id=True)
        with pytest.raises(ValueError):
            c_pe.reconfigure(i, r)


class TestCanExecute:
    def test_non_executed_upstream(self, connected_pipe_elements_2):
        rot = connected_pipe_elements_2
        assert not rot.can_execute()

    def test_executed_upstream(self, connected_pipe_elements_2):
        rot = connected_pipe_elements_2
        rot.upstream[0]._executed = True
        assert rot.can_execute()

    def test_executed_upstream_non_executed_req(
        self, connected_pipe_elements_2_requirements
    ):
        rot = connected_pipe_elements_2_requirements
        rot.upstream[0]._executed = True
        assert not rot.can_execute()

    def test_non_executed_upstream_executed_req(
        self, connected_pipe_elements_2_requirements
    ):
        rot = connected_pipe_elements_2_requirements
        rot.requirements.pop().pipe_element._executed = True
        assert not rot.can_execute()

    def test_executed_upstream_executed_req(
        self, connected_pipe_elements_2_requirements
    ):
        rot = connected_pipe_elements_2_requirements
        rot.requirements.pop().pipe_element._executed = True
        rot.upstream[0]._executed = True
        assert rot.can_execute()


class TestFlow:
    def test_attributes_post_flow_input_columns(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        assert list(base_pipe_element.input_columns) == ["a", "b", "c", "d", "e"]

    def test_attributes_post_flow_input_shape(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        assert base_pipe_element.input_dimensions == (10, 5)

    def test_attributes_post_flow_output_columns(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        assert list(base_pipe_element.output_columns) == ["a", "b", "c", "d", "e"]

    def test_attributes_post_flow_output_shape(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        assert base_pipe_element.output_dimensions == (10, 5)

    def test_non_executed_upstream(self, base_pipe_element, random_flow_data):
        random_flow_data.to_element = base_pipe_element
        with pytest.raises(AttributeError):
            base_pipe_element.flow(random_flow_data)

    def test_incorrect_addressed_flow_data(self, base_pipe_element, random_flow_data):
        base_pipe_element.upstream[0]._executed = True
        with pytest.raises(ValueError):
            base_pipe_element.flow(random_flow_data)

    def test_executed_attr_before(self, base_pipe_element, random_flow_data):
        base_pipe_element.upstream[0]._executed = True
        random_flow_data.to_element = base_pipe_element
        assert not base_pipe_element.executed

    def test_executed_attr_after(self, base_pipe_element, random_flow_data):
        base_pipe_element.upstream[0]._executed = True
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.flow(random_flow_data)
        assert base_pipe_element.executed

    def test_attributes_post_reset_input_columns(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        base_pipe_element.reset()
        assert base_pipe_element.input_columns is None

    def test_attributes_post_reset_input_shape(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        base_pipe_element.reset()
        assert base_pipe_element.input_dimensions is None

    def test_attributes_post_reset_output_columns(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        base_pipe_element.reset()
        assert base_pipe_element.output_columns is None

    def test_attributes_post_reset_output_shape(
        self, base_pipe_element, random_flow_data
    ):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        base_pipe_element.reset()
        assert base_pipe_element.output_dimensions is None

    def test_attributes_post_reset_executed(self, base_pipe_element, random_flow_data):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        base_pipe_element.flow(random_flow_data)
        base_pipe_element.reset()
        assert not base_pipe_element.executed

    def test_output_origin(self, base_pipe_element, random_flow_data):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        result = base_pipe_element.flow(random_flow_data)
        assert result[0].from_element == base_pipe_element

    def test_output_destination(self, base_pipe_element, random_flow_data):
        random_flow_data.to_element = base_pipe_element
        base_pipe_element.upstream[0]._executed = True
        result = base_pipe_element.flow(random_flow_data)
        assert result[0].to_element == base_pipe_element.downstream[0]

    def test_wrong_input(self, base_pipe_element):
        base_pipe_element.upstream[0]._executed = True
        with pytest.raises(AttributeError):
            base_pipe_element.flow([1, 2, 3, 4, 5])

    def test_wrong_input_1(self, base_pipe_element):
        base_pipe_element.upstream[0]._executed = True
        with pytest.raises(AttributeError):
            base_pipe_element.flow(None)

    def test_unconfigured_flow(self, base_pipe_element, random_flow_data):
        base_pipe_element.upstream[0]._executed = True
        c_pe = PipeElement.from_config(base_pipe_element.to_config(), element_id=True)
        random_flow_data.to_element = c_pe
        with pytest.raises(AttributeError):
            c_pe.flow(random_flow_data)
