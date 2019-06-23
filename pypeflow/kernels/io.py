"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports


# Third party imports
import numpy as np
import pandas as pd

# import pandas as pd

# Local application imports
from pypeflow.kernels.base import PipeElement


class Inlet(PipeElement):
    # TODO: add docstring

    def __init__(self, convert=True, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.convert = convert

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


class Outlet(PipeElement):
    # # TODO: add docstring

    def __init__(self, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.output = None

    def attach_downstream(self, downstream):
        raise AttributeError("Cannot attach a downstream element to an Outlet")

    def flow(self, x):
        """Sets output Variable to the result

        """
        self.output = x

    def to_config(self):
        config = super().to_config()
        config.pop("output")
        return config
