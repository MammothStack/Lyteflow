# Third party imports
import pytest
import numpy as np

# Local application imports
from lyteflow.kernels.img import *


@pytest.fixture
def image_small():
    return np.ones((2, 2))


@pytest.fixture
def image_small_padded():
    return np.array([[0, 0, 0, 0], [0, 1, 1, 0], [0, 1, 1, 0], [0, 0, 0, 0]])


@pytest.fixture
def images_small():
    return np.ones((10, 2, 2))


@pytest.fixture()
def images_small_padded():
    return np.asarray(
        [
            np.array([[0, 0, 0, 0], [0, 1, 1, 0], [0, 1, 1, 0], [0, 0, 0, 0]])
            for i in range(10)
        ]
    )


@pytest.fixture
def image_large():
    return np.ones((10, 5))


@pytest.fixture
def images_large():
    return np.ones((10, 10, 5))


@pytest.fixture
def depadder_std():
    return Depadder((2, 2), mode="m", name="Depadder_std")


@pytest.fixture()
def padder_std():
    return Padder((4, 4), mode="m", name="Padder_std")


@pytest.fixture()
def rotator_90():
    return Rotator([90])


@pytest.fixture()
def image_to_rotate():
    return np.array([[1, 2], [3, 4]])


@pytest.fixture()
def image_rotated():
    return np.array([[2, 4], [1, 3]])


@pytest.fixture()
def images_to_rotate():
    return np.array([[[1, 2], [3, 4]], [[1, 2], [3, 4]]])


@pytest.fixture()
def images_rotated():
    return np.array([[[2, 4], [1, 3]], [[2, 4], [1, 3]]])


class TestCalculateDepadValue(object):
    def test_calculate_depad_value_m_0(self, image_large):
        assert ((3, -3), (1, -2)) == calculate_depad_value(
            image_large.shape, (4, 2), mode="m"
        )

    def test_calculate_depad_value_m_1(self, image_large):
        assert ((2, -3), (1, -1)) == calculate_depad_value(
            image_large.shape, (5, 3), mode="m"
        )

    def test_calculate_depad_value_m_2(self, image_large):
        assert ((0, None), (1, -1)) == calculate_depad_value(
            image_large.shape, (10, 3), mode="m"
        )

    def test_calculate_depad_value_m_3(self, image_large):
        assert ((1, -2), (0, None)) == calculate_depad_value(
            image_large.shape, (7, 5), mode="m"
        )

    def test_calculate_depad_value_tl(self, image_large):
        assert ((0, -3), (0, -2)) == calculate_depad_value(
            image_large.shape, (7, 3), mode="tl"
        )

    def test_calculate_depad_value_tr(self, image_large):
        assert ((0, -3), (2, None)) == calculate_depad_value(
            image_large.shape, (7, 3), mode="tr"
        )

    def test_calculate_depad_value_bl(self, image_large):
        assert ((3, None), (0, -2)) == calculate_depad_value(
            image_large.shape, (7, 3), mode="bl"
        )

    def test_calculate_depad_value_br(self, image_large):
        assert ((3, None), (2, None)) == calculate_depad_value(
            image_large.shape, (7, 3), mode="br"
        )

    def test_calculate_depad_incompatible_shapes_0(self):
        with pytest.raises(ValueError):
            calculate_depad_value((4, 4), (6, 2))

    def test_calculate_depad_incompatible_shapes_1(self):
        with pytest.raises(ValueError):
            calculate_depad_value((4, 4), (2, 6))

    def test_calculate_depad_incompatible_mode(self):
        with pytest.raises(ValueError):
            calculate_depad_value((4, 4), (2, 2), mode="wrong_mode")


class TestCalculatePadValue(object):
    def test_calculate_pad_value_m(self, image_small):
        assert ((1, 1), (0, 0)) == calculate_pad_value(
            image_small.shape, (4, 2), mode="m"
        )

    def test_calculate_pad_value_m_1(self, image_small):
        assert ((1, 1), (0, 1)) == calculate_pad_value(
            image_small.shape, (4, 3), mode="m"
        )

    def test_calculate_pad_value_m_2(self, image_small):
        assert ((0, 1), (1, 1)) == calculate_pad_value(
            image_small.shape, (3, 4), mode="m"
        )

    def test_calculate_pad_value_m_3(self, image_small):
        assert ((0, 0), (1, 1)) == calculate_pad_value(
            image_small.shape, (2, 4), mode="m"
        )

    def test_calculate_pad_value_tl(self, image_small):
        assert ((0, 2), (0, 2)) == calculate_pad_value(
            image_small.shape, (4, 4), mode="tl"
        )

    def test_calculate_pad_value_tr(self, image_small):
        assert ((0, 2), (2, 0)) == calculate_pad_value(
            image_small.shape, (4, 4), mode="tr"
        )

    def test_calculate_pad_value_bl(self, image_small):
        assert ((2, 0), (0, 2)) == calculate_pad_value(
            image_small.shape, (4, 4), mode="bl"
        )

    def test_calculate_pad_value_br(self, image_small):
        assert ((2, 0), (2, 0)) == calculate_pad_value(
            image_small.shape, (4, 4), mode="br"
        )

    def test_calculate_pad_value_wrong_dimension_0(self):
        with pytest.raises(ValueError):
            calculate_pad_value((3, 3), (2, 4))

    def test_calculate_pad_value_wrong_dimension_1(self):
        with pytest.raises(ValueError):
            calculate_pad_value((3, 3), (4, 2))

    def test_calculate_pad_value_wrong_mode(self):
        with pytest.raises(ValueError):
            calculate_pad_value((2, 2), (4, 4), mode="wrong_mode")


class TestDepadder(object):
    def test_depad(self, depadder_std, image_small_padded, image_small):
        assert (
            image_small == depadder_std.depad(image_small_padded, (2, 2), mode="m")
        ).all()

    def test_depad_multiple(self, depadder_std, images_small_padded, images_small):
        assert (
            images_small == depadder_std.depad(images_small_padded, (2, 2), mode="m")
        ).all()

    def test_transform(self, depadder_std, image_small_padded, image_small):
        assert (image_small == depadder_std.transform(image_small_padded)).all()

    def test_transform_multiple(self, depadder_std, images_small_padded, images_small):
        assert (images_small == depadder_std.transform(images_small_padded)).all()

    def test_depad_dim_m(self, depadder_std, image_small_padded):
        assert (
            image_small_padded[0:-1, 1:-1]
            == depadder_std.depad(image_small_padded, (3, 2), mode="m")
        ).all()

    def test_depad_dim_m_multiple(self, depadder_std, images_small_padded):
        assert (
            images_small_padded[:, 0:-1, 1:-1]
            == depadder_std.depad(images_small_padded, (3, 2), mode="m")
        ).all()

    def test_depad_dim_tl(self, depadder_std, image_small_padded):
        assert (
            image_small_padded[0:-1, 0:-2]
            == depadder_std.depad(image_small_padded, (3, 2), mode="tl")
        ).all()

    def test_depad_dim_tl_multiple(self, depadder_std, images_small_padded):
        assert (
            images_small_padded[:, 0:-1, 0:-2]
            == depadder_std.depad(images_small_padded, (3, 2), mode="tl")
        ).all()

    def test_depad_dim_tr(self, depadder_std, image_small_padded):
        assert (
            image_small_padded[0:-1, 2:]
            == depadder_std.depad(image_small_padded, (3, 2), mode="tr")
        ).all()

    def test_depad_dim_tr_multiple(self, depadder_std, images_small_padded):
        assert (
            images_small_padded[:, 0:-1, 2:]
            == depadder_std.depad(images_small_padded, (3, 2), mode="tr")
        ).all()

    def test_depad_dim_bl(self, depadder_std, image_small_padded):
        assert (
            image_small_padded[1:, :-2]
            == depadder_std.depad(image_small_padded, (3, 2), mode="bl")
        ).all()

    def test_depad_dim_bl_multiple(self, depadder_std, images_small_padded):
        assert (
            images_small_padded[:, 1:, :-2]
            == depadder_std.depad(images_small_padded, (3, 2), mode="bl")
        ).all()

    def test_depad_dim_br(self, depadder_std, image_small_padded):
        assert (
            image_small_padded[1:, 2:]
            == depadder_std.depad(image_small_padded, (3, 2), mode="br")
        ).all()

    def test_depad_dim_br_multiple(self, depadder_std, images_small_padded):
        assert (
            images_small_padded[:, 1:, 2:]
            == depadder_std.depad(images_small_padded, (3, 2), mode="br")
        ).all()


class TestPadder(object):
    def test_pad(self, padder_std, image_small, image_small_padded):
        assert (
            image_small_padded == padder_std.pad(image_small, (4, 4), mode="m")
        ).all()

    def test_pad_multiple(self, padder_std, images_small, images_small_padded):
        assert (
            images_small_padded == padder_std.pad(images_small, (4, 4), mode="m")
        ).all()

    def test_transform(self, padder_std, image_small, image_small_padded):
        assert (image_small_padded == padder_std.transform(image_small)).all()

    def test_transform_multiple(self, padder_std, images_small, images_small_padded):
        assert (images_small_padded == padder_std.transform(images_small_padded)).all()

    def test_pad_dim_m(self, padder_std, image_small, image_small_padded):
        assert (
            image_small_padded[:, :] == padder_std.pad(image_small, (4, 4), mode="m")
        ).all()

    def test_pad_dim_m_multiple(self, padder_std, images_small, images_small_padded):
        assert (
            images_small_padded[:, :, :]
            == padder_std.pad(images_small, (4, 4), mode="m")
        ).all()

    def test_pad_dim_tl(self, padder_std, image_small, image_small_padded):
        assert (
            image_small_padded[1:, 1:] == padder_std.pad(image_small, (3, 3), mode="tl")
        ).all()

    def test_pad_dim_tl_multiple(self, padder_std, images_small, images_small_padded):
        assert (
            images_small_padded[:, 1:, 1:]
            == padder_std.pad(images_small, (3, 3), mode="tl")
        ).all()

    def test_pad_dim_tr(self, padder_std, image_small, image_small_padded):
        assert (
            image_small_padded[1:, :-1]
            == padder_std.pad(image_small, (3, 3), mode="tr")
        ).all()

    def test_pad_dim_tr_multiple(self, padder_std, images_small, images_small_padded):
        assert (
            images_small_padded[:, 1:, :-1]
            == padder_std.pad(images_small, (3, 3), mode="tr")
        ).all()

    def test_pad_dim_bl(self, padder_std, image_small, image_small_padded):
        assert (
            image_small_padded[:-1, 1:]
            == padder_std.pad(image_small, (3, 3), mode="bl")
        ).all()

    def test_pad_dim_bl_multiple(self, padder_std, images_small, images_small_padded):
        assert (
            images_small_padded[:, :-1, 1:]
            == padder_std.pad(images_small, (3, 3), mode="bl")
        ).all()

    def test_pad_dim_br(self, padder_std, image_small, image_small_padded):
        assert (
            image_small_padded[:-1, :-1]
            == padder_std.pad(image_small, (3, 3), mode="br")
        ).all()

    def test_pad_dim_br_multiple(self, padder_std, images_small, images_small_padded):
        assert (
            images_small_padded[:, :-1, :-1]
            == padder_std.pad(images_small, (3, 3), mode="br")
        ).all()


class TestRotator(object):
    def test_image_rotated(self, rotator_90, image_to_rotate, image_rotated):
        assert (image_rotated == rotator_90.transform(image_to_rotate)).all()

    def test_images_rotated(self, rotator_90, images_to_rotate, images_rotated):
        assert (images_rotated == rotator_90.transform(images_to_rotate)).all()
