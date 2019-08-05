"""Module for all PipeElements that merge multiple inputs

When the result of two or more PipeElements needs to be merged/
concatenated a specific PipeElement needs to be employed in
order to handle the multiple inputs. The _Merge class is the
super class for all PipeElements that need to merge inputs.

The _Merge class itself is also a PipeElement so the basic logic
such as creating configurations is maintained.

"""

# Standard library imports

# Third party imports
import pandas as pd
import numpy as np

# Local application imports
from lyteflow.kernels.base import PipeElement, FlowData


class _Merge(PipeElement):
    """Merging multiple inputs into a single array
    
    This class is a subclass of PipeElement and most functions of PipeElement are
    maintained. This is an abstract base class for all PipeElements that need to merge
    multiple inputs into a single data object.
    
    Methods
    ------------------
    flow(*x)
        Merge elements through transform(x)
        
    attach_upstream(*upstream)
        Attaches multiple upstream PipeElements in sequence
    
    __call__(*upstream)
        Attaches the given PipeElements in both directions
    
    """

    def __init__(self, axis=0, ignore_index=False, **kwargs):
        """Constructor

        Arguments
        ------------------
        axis : int
            The axis along which to merge the data along

        ignore_index : bool
            If the axis along which is concatenated is renamed

        """
        PipeElement.__init__(self, **kwargs)
        self.axis = axis
        self.ignore_index = ignore_index

    def flow(self, *x):
        """Receives FlowData from upstream, transforms, produces FlowData for downstream

        Before data transformations can occur, it will check that its upstream
        PipeElements have all executed, as well as it's requirement's PipeElements. In
        case of the Requirements this is essential for correct execution. It will check
        that the given FlowData elements have the correct "to_element" to ensure the data
        ended up where it was supposed to.

        The Requirements are iterated through and the defined class variables are
        changed to the values set in the defined PipeElements. Input and output shape
        and dimensions are set, as well as a class variable indicating that the
        PipeElement has executed. The FlowData is produced and returned as a list of
        length 1.

        Arguments
        ------------------
        *x : FlowData
            The input flow that should be transformed and passed downstream

        Returns
        ------------------
        x : FlowData
            A tuple of FlowData which is a the output of the data transformation that
            should be passed downstream

        Raises
        ------------------
        AttributeError
            When not all preset PipeElements and Requirements have executed

        ValueError
            When the given FlowData is not addressed to this PipeElement
        """
        self._flow_preset_check(*x)
        flow_data = FlowData(
            from_element=self,
            data=self.transform([fd.data for fd in x]),
            to_element=self.downstream[0],
        )
        self._executed = True
        self._flow_postset_check(flow_data)

        return (flow_data,)

    def attach_upstream(self, *upstream):
        """Attaches multiple upstream PipeElements in sequence

        Arguments
        ------------------
        *upstream : PipeElement
            The PipeElements that should be added to upstream source

        """
        self.upstream += upstream

    def _flow_preset_check(self, *x):
        """Checks the preset configuration and data

        Arguments
        ---------------
        x : FlowData
            The data to be checked and set

        Raises
        ------------------
        AttributeError
            When not all preset PipeElements and Requirements have executed

        ValueError
            When the given FlowData is not addressed to this PipeElement

        """
        if not self.configured:
            raise AttributeError(
                "Not configured. Requires method 'reconfigure' to "
                "reestablish connection with other PipeElements"
            )

        if not self.can_execute():
            raise AttributeError("Upstream elements or requirements are not executed")

        for fd in x:
            if fd.to_element != self:
                raise ValueError(f"{fd.to_element} does not equal {self}")

        for requirement in self.requirements:
            self.__setattr__(
                requirement.argument,
                requirement.pipe_element.__getattr__(requirement.attribute),
            )
        try:
            self.input_dimensions = [fd.data.shape for fd in x]
            self.input_columns = [fd.data.columns for fd in x]
        except AttributeError:
            pass

    def __call__(self, *upstream):
        """Attaches the given PipeElements in both directions

        Arguments
        ------------------
        *upstream : PipeElement
            PipeElements to be attached upstream

        Returns
        ------------------
        self

        """
        self.attach_upstream(*upstream)
        for element in upstream:
            element.attach_downstream(self)

        return self


class Concatenator(_Merge):
    def transform(self, x):
        def _to_numpy(df):
            try:
                return df.values
            except AttributeError:
                return np.asarray(df)

        try:
            return pd.concat(x, axis=self.axis, ignore_index=self.ignore_index)
        except TypeError:
            return np.concatenate([_to_numpy(a) for a in x], axis=self.axis)
