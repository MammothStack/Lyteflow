"""Module for splitting an input into multiple outputs

This module contains all PipeElements that handle a single input, but multiple outputs.
This occurs because the input is split along some axis into 2 or more objects or the same
object is duplicated.

"""

# Standard library imports
import warnings

# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement, FlowData


class _Split(PipeElement):
    """# TODO: Add module title

    # TODO: add module description
    
    
    

    """

    def __init__(self, n_output=None, **kwargs):
        self.n_output = n_output
        PipeElement.__init__(self, **kwargs)

    def attach_downstream(self, *downstream):
        """Attaches a downstream PipeElement

        Arguments
        ------------------
        *downstream : PipeElement
            The PipeElements that should be added to downstream


        """
        self.downstream += downstream

    def flow(self, x):
        """Receives flow from upstream, transforms and flows data to downstream elements

        Receives the flow x. The input values such as shape and columns, in case
        the input data is a pandas DataFrame.

        Arguments
        ------------------
        x : FlowData
            The input flow that should be transformed and passed downstream

        Returns
        ------------------
        *x : FlowData
            The output of the data transformation that should be passed downstream

        """

        try:
            self.input_dimensions = x.data.shape
            self.input_columns = x.data.columns
        except AttributeError:
            pass

        x = self.transform(x.data)

        if self.n_output is None:
            if len(x) != len(self.downstream):
                raise ValueError(
                    f"{len(self.downstream)} Downstream elements require "
                    f"an output, but {len(x)} were produced"
                )
        else:
            if len(self.downstream) > 1:
                raise AttributeError(
                    f"When n_output is set only 1 downstream element can be set"
                )
        try:
            self.output_dimensions = [y.shape for y in x]
            self.output_columns = [y.columns for y in x]
        except AttributeError:
            pass

        return [FlowData(self, y)]


class Duplicator(_Split):
    def __init__(self, **kwargs):
        _Split.__init__(self, **kwargs)

    def transform(self, x):
        return [x.copy() for i in range(len(self.downstream))]
