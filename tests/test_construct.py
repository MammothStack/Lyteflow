# Third party imports
import pytest
import numpy as np

# local imports
from lyteflow.construct import PipeSystem
from tests.definitions_test import PS_COMPLEX_PATH, PS_SIMPLE_PATH
from lyteflow.kernels import *


@pytest.fixture()
def images():
    return np.random.randint(1, 5, (10, 10, 10))


@pytest.fixture()
def images_fixed():
    return np.ones((10, 10, 10))


@pytest.fixture()
def labels():
    return np.random.randint(0, 10, 10)


@pytest.fixture()
def labels_fixed():
    return np.asarray([0, 2, 3, 1, 2, 3, 0, 1, 0, 2])


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
def complex_pipe_system_req():
    in_1 = Inlet(convert=False, name="in_1")
    sca = Scaler(scalar=1 / 255)(in_1)
    rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
    out_1 = Outlet(name="out_1")(rot)

    in_2 = Inlet(convert=True, name="in_2")
    cat = Categorizer(sparse=True)(in_2)
    dup = Duplicator()(cat)
    con = Concatenator()(dup)
    out_2 = Outlet(name="out_2")(con)

    dup.add_requirement(Requirement(rot, attribute="n_rotations", argument="n_result"))

    return PipeSystem(
        inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps", verbose=True
    )


@pytest.fixture()
def basic_pipesystem_json():
    return PipeSystem.from_json(json_file_name=PS_SIMPLE_PATH)


@pytest.fixture()
def complex_pipesystem_json():
    return PipeSystem.from_json(json_file_name=PS_COMPLEX_PATH)


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
        self, basic_pipesystem, basic_pipesystem_json, images_fixed, labels_fixed
    ):
        res = basic_pipesystem.flow(images_fixed, labels_fixed)
        res_json = basic_pipesystem_json.flow(images_fixed, labels_fixed)

        assert (res[0] == res_json[0]).all() and (res[1] == res_json[1]).all().all()

    def tests_to_from_config_req(self, complex_pipe_system_req, images, labels):
        before = complex_pipe_system_req.flow(images, labels)
        after = PipeSystem.from_config(complex_pipe_system_req.to_config()).flow(
            images, labels
        )

        assert (before[0] == after[0]).all() and (before[1] == after[1]).all().all()


class TestInlets:
    def test_correct_inlets(self, complex_pipesystem):
        in_1 = Inlet(convert=False, name="in_1")
        sca = Scaler(scalar=1 / 255)(in_1)
        rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
        out_1 = Outlet(name="out_1")(rot)

        in_2 = Inlet(convert=True, name="in_2")
        cat = Categorizer(sparse=True)(in_2)
        dup = Duplicator()(cat)
        con = Concatenator()(dup, dup, dup)
        out_2 = Outlet(name="out_2")(con)

        ps = PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps")
        assert ps.inlets[0] == in_1 and ps.inlets[1] == in_2

    def test_incorrect_inputs(self, complex_pipesystem, images, labels):
        images_2 = images.copy()
        with pytest.raises(ValueError):
            complex_pipesystem.flow(images, images_2, labels)

    def test_give_wrong_pipe_element(self):
        in_1 = Inlet(convert=False, name="in_1")
        out_1 = Outlet(name="out_1")(in_1)

        in_2 = PipeElement(convert=True, name="in_2")
        out_2 = Outlet(name="out_2")(in_2)

        with pytest.raises(TypeError):
            ps = PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps")


class TestOutlets:
    def test_correct_outlets(self, complex_pipesystem):
        in_1 = Inlet(convert=False, name="in_1")
        sca = Scaler(scalar=1 / 255)(in_1)
        rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
        out_1 = Outlet(name="out_1")(rot)

        in_2 = Inlet(convert=True, name="in_2")
        cat = Categorizer(sparse=True)(in_2)
        dup = Duplicator()(cat)
        con = Concatenator()(dup, dup, dup)
        out_2 = Outlet(name="out_2")(con)

        ps = PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps")
        assert ps.outlets[0] == out_1 and ps.outlets[1] == out_2

    def test_correct_output_from_flow(self, complex_pipesystem, images, labels):
        results = complex_pipesystem.flow(images, labels)
        assert len(results) == len(complex_pipesystem.outlets)

    def test_give_wrong_pipe_element(self):
        in_1 = Inlet(convert=False, name="in_1")
        out_1 = Outlet(name="out_1")(in_1)

        in_2 = Inlet(convert=True, name="in_2")
        out_2 = PipeElement(name="out_2")(in_2)

        with pytest.raises(TypeError):
            ps = PipeSystem(inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps")


class TestExecutionSequence:
    def test_order(self):
        in_1 = Inlet(convert=False, name="in_1")
        sca = Scaler(scalar=1 / 255)(in_1)
        rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
        out_1 = Outlet(name="out_1")(rot)

        in_2 = Inlet(convert=True, name="in_2")
        cat = Categorizer(sparse=True)(in_2)
        dup = Duplicator()(cat)
        con = Concatenator()(dup)
        out_2 = Outlet(name="out_2")(con)

        dup.add_requirement(
            Requirement(rot, attribute="n_rotations", argument="n_result")
        )

        ps = PipeSystem(
            inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps", verbose=True
        )
        assert ps.execution_sequence.index(dup) > ps.execution_sequence.index(rot)

    def test_dead_lock_pipesystem(self):

        in_1 = Inlet(convert=False, name="in_1")
        sca = Scaler(scalar=1 / 255)(in_1)
        rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
        out_1 = Outlet(name="out_1")(rot)

        in_2 = Inlet(convert=True, name="in_2")
        cat = Categorizer(sparse=True)(in_2)
        dup = Duplicator()(cat)
        con = Concatenator()(dup)
        out_2 = Outlet(name="out_2")(con)

        dup.add_requirement(
            Requirement(rot, attribute="n_rotations", argument="n_result")
        )

        rot.add_requirement(
            Requirement(con, attribute="ignore_index", argument="remove_padding")
        )
        with pytest.raises(AttributeError):
            ps = PipeSystem(
                inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps", verbose=True
            )


class TestContains:
    def test_contains(self):
        in_1 = Inlet(convert=False, name="in_1")
        sca = Scaler(scalar=1 / 255)(in_1)
        rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
        out_1 = Outlet(name="out_1")(rot)

        in_2 = Inlet(convert=True, name="in_2")
        cat = Categorizer(sparse=True)(in_2)
        dup = Duplicator()(cat)
        con = Concatenator()(dup)
        out_2 = Outlet(name="out_2")(con)

        dup.add_requirement(
            Requirement(rot, attribute="n_rotations", argument="n_result")
        )

        ps = PipeSystem(
            inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps", verbose=True
        )

        assert cat in ps

    def test_not_contains(self, complex_pipesystem):
        assert PipeElement() not in complex_pipesystem


class TestLength:
    def test_length(self, complex_pipesystem):
        assert len(complex_pipesystem) == 9

    def test_length_2(self, basic_pipesystem):
        assert len(basic_pipesystem) == 4


class TestAddition:
    def test_length(self, complex_pipesystem, complex_pipe_system_req):
        added = complex_pipesystem + complex_pipe_system_req
        assert len(added) == 18

    def test_length_2(self, complex_pipesystem, basic_pipesystem):
        added = complex_pipesystem + basic_pipesystem
        assert len(added) == 13

    def test_length_inlets(self, complex_pipesystem, complex_pipe_system_req):
        added = complex_pipesystem + complex_pipe_system_req
        assert len(added.inlets) == 4

    def test_length_outlets(self, complex_pipesystem, complex_pipe_system_req):
        added = complex_pipesystem + complex_pipe_system_req
        assert len(added.inlets) == 4

    def test_incorrect_type(self, complex_pipe_system_req):
        with pytest.raises(TypeError):
            added = complex_pipe_system_req + 5


class TestMultiplication:
    def test_length(self, complex_pipesystem, complex_pipe_system_req):
        added = complex_pipesystem * complex_pipe_system_req
        assert len(added) == 16

    def test_length_2(self, complex_pipesystem, basic_pipesystem):
        added = complex_pipesystem * basic_pipesystem
        assert len(added) == 11

    def test_length_inlets(self, complex_pipesystem, complex_pipe_system_req):
        added = complex_pipesystem * complex_pipe_system_req
        assert len(added.inlets) == 2

    def test_length_outlets(self, complex_pipesystem, complex_pipe_system_req):
        added = complex_pipesystem * complex_pipe_system_req
        assert len(added.inlets) == 2

    def test_incorrect_type(self, complex_pipe_system_req):
        with pytest.raises(TypeError):
            added = complex_pipe_system_req * 5

    def test_mismatch_inlet_outlet(self, complex_pipesystem):
        in_1 = Inlet(convert=False, name="in_1")
        out_1 = Outlet(name="out_1")(in_1)

        in_2 = Inlet(convert=True, name="in_2")
        out_2 = Outlet(name="out_2")(in_2)

        in_3 = Inlet(name="in_3")
        out_3 = Outlet(name="out_3")(in_3)
        ps = PipeSystem(
            inlets=[in_1, in_2, in_3], outlets=[out_1, out_2, out_3], name="ps"
        )

        with pytest.raises(AttributeError):
            new_ps = ps * complex_pipesystem


class TestAttributes:
    def test_before_flow_input_columns(self, complex_pipesystem):
        assert complex_pipesystem.input_columns is None

    def test_before_flow_input_shape(self, complex_pipesystem):
        assert complex_pipesystem.input_dimensions is None

    def test_before_flow_output_columns(self, complex_pipesystem):
        assert complex_pipesystem.output_columns is None

    def test_before_flow_output_shape(self, complex_pipesystem):
        assert complex_pipesystem.output_dimensions is None

    def test_after_flow_input_columns(self, complex_pipesystem, images, labels_fixed):
        complex_pipesystem.flow(images, labels_fixed)
        assert complex_pipesystem.input_columns == [None, [0]]

    def test_after_flow_input_shape(self, complex_pipesystem, images, labels_fixed):
        complex_pipesystem.flow(images, labels_fixed)
        assert complex_pipesystem.input_dimensions == [(10, 10, 10), (10,)]

    def test_after_flow_output_columns(self, complex_pipesystem, images, labels_fixed):
        complex_pipesystem.flow(images, labels_fixed)
        assert complex_pipesystem.output_columns == [None, ["0_0", "0_1", "0_2", "0_3"]]

    def test_after_flow_output_shape(self, complex_pipesystem, images, labels_fixed):
        complex_pipesystem.flow(images, labels_fixed)
        assert complex_pipesystem.output_dimensions == [(30, 10, 10), (30, 4)]

    def test_after_flow_input_columns_after_reset(
        self, complex_pipesystem, images, labels_fixed
    ):
        complex_pipesystem.flow(images, labels_fixed)
        complex_pipesystem.reset()
        assert complex_pipesystem.input_columns is None

    def test_after_flow_input_shape_after_reset(
        self, complex_pipesystem, images, labels_fixed
    ):
        complex_pipesystem.flow(images, labels_fixed)
        complex_pipesystem.reset()
        assert complex_pipesystem.input_dimensions is None

    def test_after_flow_output_columns_after_reset(
        self, complex_pipesystem, images, labels_fixed
    ):
        complex_pipesystem.flow(images, labels_fixed)
        complex_pipesystem.reset()
        assert complex_pipesystem.output_columns is None

    def test_after_flow_output_shape_after_reset(
        self, complex_pipesystem, images, labels_fixed
    ):
        complex_pipesystem.flow(images, labels_fixed)
        complex_pipesystem.reset()
        assert complex_pipesystem.output_dimensions is None

    def test_before_flow_executed(self, complex_pipesystem, images, labels_fixed):
        assert not complex_pipesystem.executed

    def test_after_flow_executed(self, complex_pipesystem, images, labels_fixed):
        complex_pipesystem.flow(images, labels_fixed)
        assert complex_pipesystem.executed

    def test_after_flow_executed_after_reset(
        self, complex_pipesystem, images, labels_fixed
    ):
        complex_pipesystem.flow(images, labels_fixed)
        complex_pipesystem.reset()
        assert not complex_pipesystem.executed

    def test_before_flow_element_attributes(
        self, complex_pipesystem, images_fixed, labels_fixed
    ):
        complex_pipesystem.flow(images_fixed, labels_fixed)
        assert all(
            [e.input_dimensions is not None for e in complex_pipesystem.all_elements]
        )

    def test_after_flow_element_attributes(
        self, complex_pipesystem, images_fixed, labels_fixed
    ):
        complex_pipesystem.flow(images_fixed, labels_fixed)
        complex_pipesystem.reset()
        assert all(
            [e.input_dimensions is None for e in complex_pipesystem.all_elements]
        )
