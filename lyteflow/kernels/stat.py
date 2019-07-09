"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports

# Third party imports
import pandas as pd
import numpy as np

# Local application imports
from lyteflow.kernels.base import PipeElement


def _handle_zero_scalar(scalar):
    """Transforms all 0 occurrences in scalar or list of scalars

    Arguments
    ------------------
    scalar : int / float / np.ndarray
        The scalar or list of scalars to be transformed

    """
    if isinstance(scalar, np.ndarray):
        scalar[scalar == 0.0] = 1.0
    elif np.isscalar(scalar):
        scalar = 1.0 if scalar == 0.0 else scalar
    return scalar


class Normalizer(PipeElement):
    """Normalizes the input between two values

    This PipeElement takes an input and normalizes the values across a set minimum and
    maximum. The minimum and maximum range from where the values should be scaled from
    are determined automatically or can be set as a parameter scale_from.

    Once the values are normalized to min=0 and max=1 the values can be scaled to a
    different range. The default of this range is (0, 1).

    The columns of the given input can be normalized either dependently or
    independently through the keyword: dependent. When dependent is set to true the
    minimums and maximums are applied across all columns jointly. When dependent is set
    to false the minimums and maximums are applied individually

    Arguments
    ------------------
    columns : list (default=None)
        The columns of the given input that should be normalized. If None then all
        columns of the given input will be used.

    dependent : bool (default=False)
        If the columns of the given input should be transformed dependent on the others

    scale_from : tuple
        The minimum, maximum from where the values are scaled from

    scale_to : tuple
        The minimum, maximum from where the values are scaled to

    upstream : PipeElement
        The pipe element which is connected upstream, meaning the upstream element will
        flow data to this element

    downstream : PipeElement
        The pipe element which is connected downstream, meaning this pipe element will
        flow data to the downstream element

    name : str
        The name that should be given to the PipeElement

    Methods
    ------------------
    transform(x)
        Returns normalized data based on the given columns

    flow(x)
        Method that is called when passing data to next PipeElement

    attach_upstream(upstream)
        Attaches the given PipeElement as an upstream flow source

    attach_downstream(downstream)
        Attaches the given PipeElement as a downstream flow destination

    to_config()
        Creates serializable PipeElement

    Raises
    ------------------
    ValueError
        When given range scale_from/scale_to are not in the correct format
    """

    def __init__(
        self,
        columns=None,
        dependent=False,
        scale_from=(None, None),
        scale_to=(0, 1),
        **kwargs
    ):
        PipeElement.__init__(self, **kwargs)
        self.columns = columns
        self.dependent = dependent
        if len(scale_from) != 2 or scale_from[1] > scale_from[0]:
            raise ValueError(
                """scale_from should be a 2-tuple where n=0 should be the  
                minimum value and n=1 the maximum value"""
            )
        self.scale_from = scale_from
        if len(scale_to) != 2 or scale_to[1] > scale_to[0]:
            raise ValueError(
                """scale_to should be a 2-tuple where n=0 should be the  
                minimum value and n=1 the maximum value"""
            )
        self.scale_to = scale_to

    def transform(self, x):
        """Returns normalized data based on the given columns

        Arguments
        ------------------
        x : pd.DataFrame/np.ndarray
            The input which should be normalized

        Returns
        ------------------
        x : DataFrame
            The normalized Data

        """
        x_val = x.values
        axis = None if self.dependent else 0

        minimum = (
            np.nanmin(x_val, axis=axis)
            if self.scale_from[0] is None
            else self.scale_from[0]
        )
        maximum = (
            np.nanmax(x_val, axis=axis)
            if self.scale_from[1] is None
            else self.scale_from[1]
        )

        x_val = ((x_val - minimum) / _handle_zero_scalar(maximum - minimum)) * (
            self.scale_to[1] - self.scale_to[0]
        ) + self.scale_to[0]

        return pd.DataFrame(x_val, columns=x.columns + ":normalized")


class Standardizer(PipeElement):
    """
    # TODO
    """

    def __init__(self, dependent=False, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.dependent = dependent

    def transform(self, x):
        x_val = x.values
        axis = None if self.dependent else 0
        mean = np.nanmean(x_val, axis=axis)
        std = np.nanstd(x_val, axis=axis)
        return pd.DataFrame(
            (x_val - mean) / _handle_zero_scalar(std),
            columns=x.columns + ":standardized",
        )


class Scaler(PipeElement):
    """
    ' TODO
    """

    def __init__(self, scalar, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.scalar = scalar

    def transform(self, x):
        try:
            return pd.DataFrame(x.values * self.scalar)
        except AttributeError:
            return x * self.scalar
