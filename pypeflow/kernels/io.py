"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports


# Third party imports
import numpy as np

# import pandas as pd

# Local application imports
from pypeflow.base import Basic


class Inlet(Basic):
    # TODO: add docstring

    def __init__(self, convert=True, **kwargs):
        self.convert = convert
        Basic.__init__(self, **kwargs)

    def transform(self, x):
        if isinstance(x, list):
            x = np.asarray(x)
            self.input_dimensions = x.shape
        if len(x.shape) == 1 or len(x.shape) == 2:
            return pd.DataFrame(x)
        else:
            return x

    def attach_upstream(self, upstream):
        raise AttributeError("Cannot attach an upstream element to an Inlet")


class Outlet(Basic):
    # # TODO: add docstring
    def attach_downstream(self, downstream):
        raise AttributeError("Cannot attach a downstream element to an Outlet")
