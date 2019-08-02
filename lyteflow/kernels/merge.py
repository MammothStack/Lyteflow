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
from lyteflow.kernels.base import PipeElement


class _Merge(PipeElement):
    """Merging multiple inputs into a single array
    
    This class is a subclass of PipeElement and most functions
    of PipeElement are maintained. This is an abstract base class
    for all PipeElements that need to merge multiple inputs into
    a single data object.
    
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
        """Merge elements through transform(x)

        Receives the flow x. In case not all upstream PipeElements
        have flown their data to this, then the data will be stored
        in a list. This list is then transformed via the transform
        function and the result passed downstream

        Arguments
        ------------------
        *x : numpy array/pandas DataFrame
            The input flow that should be transformed and passed downstream

        """
        ordered_reservoir = []
        
        for up in self.upstream:
            for i in x:
                if up == i[0]:
                    ordered_reservoir.append(i[1])
                    
        if len(ordered_reservoir) != len(self.upstream):
            raise ValueError("Given data could not be fitted to upstream elements")

        try:
            self.input_dimensions = [x.shape for x in ordered_reservoir]
            self.input_columns = [x.columns for x in ordered_reservoir]
        except AttributeError:
            pass

        for data in ordered_reservoir:
            if len(data.shape) <= self.axis:
                raise AttributeError(
                    f"Given data has only {len(data.shape)} axes, "
                    f"but requires at least {self.axis}"
                )

        x = self.transform(ordered_reservoir)

        try:
            self.output_dimensions = x.shape
            self.output_columns = x.columns
        except AttributeError:
            pass
            
        return self.downstream[0], x

    def attach_upstream(self, *upstream):
        """Attaches multiple upstream PipeElements in sequence

        Arguments
        ------------------
        *upstream : PipeElement
            The PipeElements that should be added to upstream source

        """
        self.upstream += upstream

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
