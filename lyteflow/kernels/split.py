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
        """Constructor for _Split

        TODO

        """
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
        """Receives FlowData from upstream, transforms, produces FlowData for downstream

        Before data transformations can occur, it will check that its upstream
        PipeElements have all executed, as well as it's requirement's PipeElements. In
        case of the Requirements this is essential for correct execution. It will check
        that the given FlowData element has the correct "to_element" to ensure the data
        ended up where it was supposed to.

        The Requirements are iterated through and the defined class variables are
        changed to the values set in the defined PipeElements. Input and output shape
        and dimensions are set, as well as a class variable indicating that the
        PipeElement has executed. The FlowData is produced and returned as a list of
        the length of the downstream PipeElements or the length set by the n_output
        variable

        Arguments
        ------------------
        x : FlowData
            The input flow that should be transformed and passed downstream

        Returns
        ------------------
        *x : FlowData
            A tuple of FlowData which is a the output of the data transformation that
            should be passed downstream

        Raises
        ------------------
        AttributeError
            When not all preset PipeElements and Requirements have executed

        ValueError
            When the given FlowData is not addressed to this PipeElement

        AttributeError
            When n_output variable is set and multiple downstream elements are set

        ValueError
            When the amount of data produced does not match the set downstream elements

        """
        self._flow_preset_check(x)
        transformed_x = self.transform(x.data)
        self._flow_postset_check(*transformed_x)
        self._executed = True

        if self.n_output is None:
            matched = zip([y for y in transformed_x], [d for d in self.downstream])
            return tuple(
                FlowData(from_element=self, data=z[0], to_element=z[1]) for z in matched
            )
        else:
            return tuple(
                FlowData(from_element=self, data=y, to_element=self.downstream[0])
                for y in transformed_x
            )

    def _flow_postset_check(self, *x):
        """Checks the postset configuration and data

        Arguments
        ---------------
        *x : numpy.array/pandas.DataFrame
            Tuple of data from transform method

        Raises
        ------------------
        AttributeError
            When n_output variable is set and multiple downstream elements are set

        ValueError
            When the amount of data produced does not match the set downstream elements


        """

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


class Duplicator(_Split):
    def __init__(self, **kwargs):
        _Split.__init__(self, **kwargs)

    def transform(self, x):
        return [x.copy() for i in range(len(self.downstream))]
