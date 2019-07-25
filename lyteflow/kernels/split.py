"""Module for splitting an input into multiple outputs

This module contains all PipeElements that handle a single
input, but multiple outputs. This occurs because the input
is split along some axis into 2 or more objects or the same
object is duplicated.

"""

# Standard library imports

# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement


class _Split(PipeElement):
    """# TODO: Add module title

    # TODO: add module description
    
    
    

    """

    def __init__(self, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.downstream = []

    def attach_downstream(self, *downstream):
        """

        Arguments
        ------------------
        *downstream : PipeElement
            The PipeElements that should be added to downstream


        """
        self.downstream = self.downstream + list(downstream)

    def flow(self, x):
        """TODO

        """

        try:
            self.input_dimensions = x.shape
            self.input_columns = x.columns
        except AttributeError:
            pass

        x = self.transform(x)

        try:
            self.output_dimensions = [y.shape for y in x]
            self.output_columns = [y.columns for y in x]
        except AttributeError:
            pass

        for i in range(len(self.downstream)):
            self.downstream[i].flow(x[i])


class Duplicator(_Split):
    def __init__(self, **kwargs):
        _Split.__init__(self, **kwargs)

    def transform(self, x):
        return [x.copy() for i in range(len(self.downstream))]
