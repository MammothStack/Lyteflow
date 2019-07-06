"""This module is for PipeElements that transform image data

Everything from adding image rotation, adding and removing padding to fit a desired
resolution, upsampling, downsampling, Channel addition and removal can be found in
this module

# TODO: Add Upsampler
# TODO: Add Downsampler
# TODO: Add ChannelAdder
# TODO: Add ChannelFlattener

"""
# Standard library imports

# Third party imports
import numpy as np
from scipy.ndimage.interpolation import rotate

# Local application imports
from pypeflow.kernels.base import PipeElement


def calculate_depad_value(input_shape, output_shape, mode="m"):
    """Calculates the values needed for depadding

    Arguments
    ------------------
    input_shape : tuple
        The input resolution of the image to be depadded, where (height, width)

    output_shape : tuple
        The output resolution of the image after depadding where (height, width)

    mode : str (default="m")
        One of the following string values

        "m"
            Removes the pixels from the outside in. When only one pixel should be
            removed from an axis the bottom and right sides will be preferred
        "tl"
            Preserves the top-left corner and depads from the bottom-right
        "tr"
            Preserves the top-right corner and depads from the bottom-left
        "bl"
            Preserves the bottom-left corner and depads from the top-right
        "br"
            Preserves the bottom-right corner and depads from the top-left

    Returns
    ------------------
    depad_value : tuple
        A pair of tuples where the values equate to ((top, bottom), (left, right))

    Raises
    ------------------
    ValueError
        When a dimension of the desired resolution (output_shape) is larger than the
        resolution of the input (input_shape)

    """
    diff = np.subtract(input_shape, output_shape)
    if (diff < 0).any():
        raise ValueError(
            f"Given resolution {output_shape} is too large for image resolution "
            f"{input_shape}"
        )
    if mode == "m":
        diff = diff / 2
        return (
            (
                np.floor(diff[0]).astype(int),
                -np.ceil(diff[0]).astype(int) if diff[0] != 0 else None,
            ),
            (
                np.floor(diff[1]).astype(int),
                -np.ceil(diff[1]).astype(int) if diff[1] != 0 else None,
            ),
        )

    elif mode == "tl":
        return (0, -diff[0]), (0, -diff[1])
    elif mode == "tr":
        return (0, -diff[0]), (diff[1], None)
    elif mode == "bl":
        return (diff[0], None), (0, -diff[1])
    elif mode == "br":
        return (diff[0], None), (diff[1], None)
    else:
        raise ValueError(f"mode {mode} is not recognized as a valid mode")


def calculate_pad_value(input_shape, output_shape, mode="m"):
    """Calculates the values needed for padding

    Arguments
    ------------------
    input_shape : tuple
        The input resolution of the image to be padded, where (height, width)

    output_shape : tuple
        The output resolution of the image after padding, where (height, width)

    mode : str (default="m")
        One of the following string values

        "m"
            Adds the pixels from the inside out. When only one pixel should be
            added to an axis the bottom and right sides will be preferred
        "tl"
            Preserves the top-left corner and pads to the bottom-right
        "tr"
            Preserves the top-right corner and pads to the bottom-left
        "bl"
            Preserves the bottom-left corner and pads to the top-right
        "br"
            Preserves the bottom-right corner and pads to the top-left

    Returns
    ------------------
    pad_value : tuple
        A pair of tuples where the values equate to ((top, bottom), (left, right))

    Raises
    ------------------
    ValueError
        When a dimension of the desired resolution (output_shape) is smaller than the
        resolution of the input (input_shape)

    """
    diff = np.subtract(output_shape, input_shape)
    if (diff < 0).any():
        raise ValueError(
            f"Given resolution {output_shape} is too small for image resolution "
            f"{input_shape}"
        )
    if mode == "m":
        diff = diff / 2
        return (
            (np.floor(diff[0]).astype(int), np.ceil(diff[0]).astype(int)),
            (np.floor(diff[1]).astype(int), np.ceil(diff[1]).astype(int)),
        )
    elif mode == "tl":
        return (0, diff[0]), (0, diff[1])
    elif mode == "tr":
        return (0, diff[0]), (diff[1], 0)
    elif mode == "bl":
        return (diff[0], 0), (0, diff[1])
    elif mode == "br":
        return (diff[0], 0), (diff[1], 0)
    else:
        raise ValueError(f"mode {mode} is not recognized as a valid mode")


class Rotator(PipeElement):
    """Rotates the given image input by the given degrees

    This class is a subclass of PipeElement. The Rotator is responsible for rotating
    a given image along 1 axis. If the length and width of the image is the x and y
    axis then image is rotated at the center of those two axes.

    The amount of rotation is determined by the given input when instantiating the
    class. The rotations should be given as integers with a range from -359 to 359.
    Any values over this will be converted so a 370 degree rotation will be
    converted to 10. Then all duplicate rotations will be removed.

    During rotation if the image is not rotated at 90, 180, or 270 degrees, some
    additional padding will be introduced in order to accommodate the rotation. This
    can be removed, but some data loss may occur.

    Arguments
    ------------------
    rotations : list or set
        A list like object containing the degrees of the required rotations from
        -359 to 359 degrees

    remove_padding : bool
        If the padding, which is added during rotation should be removed after
        rotating

    keep_original : bool
        If the original images should be included in the resulting array

    upstream : PipeElement
        The pipe element which is connected upstream, meaning the upstream
        element will flow data to this element

    downstream : PipeElement
        The pipe element which is connected downstream, meaning this pipe
        element will flow data to the downstream element

    name : str
        The name that should be given to the PipeElement

    Methods
    ------------------
    transform(x)
        Returns the original as well as the rotated images

    flow(x)
        Method that is called when passing data to next PipeElement

    attach_upstream(upstream)
        Attaches the given PipeElement as an upstream flow source

    attach_downstream(downstream)
        Attaches the given PipeElement as a downstream flow destination

    to_config()
        Creates serializable PipeElement
    """

    def __init__(
        self, rotations=None, remove_padding=False, keep_original=False, **kwargs
    ):
        PipeElement.__init__(self, **kwargs)
        verified_rotations = [r % 360 if r >= 0 else r % -360 for r in rotations]

        if keep_original:
            verified_rotations.append(0)

        self.keep_original = keep_original
        self.rotations = sorted(set(verified_rotations))
        self.remove_padding = remove_padding

    def transform(self, x):
        """Rotates the input and returns the rotations

        Overrides the transform method of the super class PipeElement. It iterates through
        the rotations given during initialization and rotates each image in x with that
        rotation. The resulting arrays are collected in a list in concatenated into a
        singular numpy array

        Arguments
        ------------------
        x : np.ndarray
            The images to be rotated

        Returns
        ------------------
        rotated : np.ndarray
            The images with the given rotations

        """
        if len(x.shape) == 2:
            maximum_resolution = np.max(
                [rotate(x, r).shape for r in self.rotations], axis=0
            )
            rotated = np.array(
                [
                    Padder.pad(image=rotate(x, rotation), resolution=maximum_resolution)
                    for rotation in self.rotations
                ]
            )
        elif len(x.shape) == 3:
            maximum_resolution = np.max(
                [rotate(x[0], r).shape for r in self.rotations], axis=0
            )
            rotated = np.array(
                [
                    Padder.pad(
                        image=rotate(img, rotation), resolution=maximum_resolution
                    )
                    for rotation in self.rotations
                    for img in x
                ]
            )
        else:
            raise ValueError(
                f"Given images need to be of shape (n, height, width) or ("
                f"height, width), {x.shape} was given"
            )

        # concatenated = np.concatenate(arrays=rotated, axis=0)
        if self.remove_padding:
            return Depadder.depad(rotated, resolution=x[0].shape)
        else:
            return rotated


class Padder(PipeElement):
    """Adds padding to the given images

    Arguments
    ------------------
    resolution : tuple
        The resolution that should result from padding

    mode : str
        One of the following string values

        "m"
            Adds the pixels from the inside out. When only one pixel should be
            added to an axis the bottom and right sides will be preferred
        "tl"
            Preserves the top-left corner and pads to the bottom-right
        "tr"
            Preserves the top-right corner and pads to the bottom-left
        "bl"
            Preserves the bottom-left corner and pads to the top-right
        "br"
            Preserves the bottom-right corner and pads to the top-left

    upstream : PipeElement
        The pipe element which is connected upstream, meaning the upstream
        element will flow data to this element

    downstream : PipeElement
        The pipe element which is connected downstream, meaning this pipe
        element will flow data to the downstream element

    name : str
        The name that should be given to the PipeElement

    Methods
    ------------------
    transform(x)
        Returns the original as well as the rotated images

    flow(x)
        Method that is called when passing data to next PipeElement

    attach_upstream(upstream)
        Attaches the given PipeElement as an upstream flow source

    attach_downstream(downstream)
        Attaches the given PipeElement as a downstream flow destination

    to_config()
        Creates serializable PipeElement
    """

    def __init__(self, resolution, mode="m", **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.resolution = resolution
        self.mode = mode

    @staticmethod
    def pad(image, resolution, mode="m"):
        """Adds padding of the given image(s) to the given resolution

        Arguments
        ------------------
        image : np.ndarray
            Image(s) that should be padded. Should be in the format (n, height,
            width) or (height, width)

        resolution : tuple
            The resolution that should result from padding

        mode : str
            One of the following string values

            "m"
                Adds the pixels from the inside out. When only one pixel should be
                added to an axis the bottom and right sides will be preferred
            "tl"
                Preserves the top-left corner and pads to the bottom-right
            "tr"
                Preserves the top-right corner and pads to the bottom-left
            "bl"
                Preserves the bottom-left corner and pads to the top-right
            "br"
                Preserves the bottom-right corner and pads to the top-left

        Returns
        ------------------
        padded : np.ndarray
            Padded image(s) with the format (resolution) or (n, resolution)

        """
        if len(image.shape) == 2:
            input_shape = image.shape
        elif len(image.shape) == 3:
            input_shape = image[0].shape
        else:
            raise ValueError(
                f"Images need to be either (height, width) or (n, height, width) where "
                f"n is the amount of images"
            )
        (top, bottom), (left, right) = calculate_pad_value(
            input_shape=input_shape, output_shape=resolution, mode=mode
        )
        if len(image.shape) == 2:
            return np.pad(image, ((top, bottom), (left, right)), mode="constant")
        else:
            return np.array(
                [
                    np.pad(img, ((top, bottom), (left, right)), mode="constant")
                    for img in image
                ]
            )

    def transform(self, x):
        return self.pad(image=x, resolution=self.resolution, mode=self.mode)


class Depadder(PipeElement):
    """Removes padding from the given images

    Arguments
    ------------------
    resolution : tuple
        The resolution that should result from depadding

    mode : str
        One of the following string values

        "m"
            Removes the pixels from the outside in. When only one pixel should be
            removed from an axis the bottom and right sides will be preferred
        "tl"
            Preserves the top-left corner and depads from the bottom-right
        "tr"
            Preserves the top-right corner and depads from the bottom-left
        "bl"
            Preserves the bottom-left corner and depads from the top-right
        "br"
            Preserves the bottom-right corner and depads from the top-left

    upstream : PipeElement
        The pipe element which is connected upstream, meaning the upstream
        element will flow data to this element

    downstream : PipeElement
        The pipe element which is connected downstream, meaning this pipe
        element will flow data to the downstream element

    name : str
        The name that should be given to the PipeElement

    Methods
    ------------------
    transform(x)
        Returns the original as well as the rotated images

    flow(x)
        Method that is called when passing data to next PipeElement

    attach_upstream(upstream)
        Attaches the given PipeElement as an upstream flow source

    attach_downstream(downstream)
        Attaches the given PipeElement as a downstream flow destination

    to_config()
        Creates serializable PipeElement
    """

    def __init__(self, resolution, mode="m", **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.resolution = resolution
        self.mode = mode

    @staticmethod
    def depad(image, resolution, mode="m"):
        """Removes padding of the given image(s) to the given resolution

        Arguments
        ------------------
        image : np.ndarray
            Image(s) that should be depadded. Should be in the format (n, height,
            width) or (height, width)

        resolution : tuple
            The resolution that should result from depadding

        mode : str
            One of the following string values

            "m"
                Removes the pixels from the outside in. When only one pixel should be
                removed from an axis the bottom and right sides will be preferred
            "tl"
                Preserves the top-left corner and depads from the bottom-right
            "tr"
                Preserves the top-right corner and depads from the bottom-left
            "bl"
                Preserves the bottom-left corner and depads from the top-right
            "br"
                Preserves the bottom-right corner and depads from the top-left

        Returns
        ------------------
        depadded : np.ndarray
            Depadded image(s) with the format (resolution) or (n, resolution)

        """
        if len(image.shape) == 2:
            input_shape = image.shape
        elif len(image.shape) == 3:
            input_shape = image[0].shape
        else:
            raise ValueError(
                f"Images need to be either (height, width) or (n, height, width) where "
                f"n is the amount of images"
            )

        (top, bottom), (left, right) = calculate_depad_value(
            input_shape=input_shape, output_shape=resolution, mode=mode
        )
        if len(image.shape) == 2:
            return image[top:bottom, left:right]
        else:
            return image[:, top:bottom, left:right]

    def transform(self, x):
        return self.depad(image=x, resolution=self.resolution, mode=self.mode)


class Upsampler(PipeElement):
    """

    """

    pass


class Downsampler(PipeElement):
    """

    """

    pass
