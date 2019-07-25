"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports

# Third party imports
import pandas as pd
import numpy as np

# Local application imports
from lyteflow.kernels.base import PipeElement


class _Merge(PipeElement):
    """Merging multiple inputs into a single array"""
    
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
        """Merge Elements through transform(x)

        Receives the flow x. In case not all upstream PipeElements
        have flown their data to this, then the data will be stored
        in a list. This list is then transformed via the transform
        function and the result passed downstream

        Arguments
        ------------------
        *x : numpy array/pandas DataFrame
            The input flow that should be transformed and passed downstream

        """
        for 
        
        self._reservoir.append(x)

        if len(self._reservoir) == len(self.upstream):
            try:
                self.input_dimensions = [x.shape for x in self._reservoir]
                self.input_columns = [x.columns for x in self._reservoir]
            except AttributeError:
                pass

            for data in self._reservoir:
                if len(data.shape) < self.axis:
                    raise AttributeError(
                        f"Given data has only {len(data.shape)} axes, "
                        f"but requires at least {self.axis}"
                    )

            x = self.transform(self._reservoir)

            try:
                self.output_dimensions = x.shape
                self.output_columns = x.columns
            except AttributeError:
                pass

            self.downstream.flow(x)
            self._reservoir = []

    def attach_upstream(self, *upstream):
        """Attaches multiple upstream PipeElements in sequence

        Arguments
        ------------------
        *upstream : PipeElement
            The PipeElements that should be added to upstream source

        """
        self.upstream = self.upstream + list(upstream)

    def to_config(self):
        config = super().to_config()
        config["attributes"].pop("_reservoir")
        return config

    def __call__(self, *upstream):
        """Attaches the given PipeElement in both directions

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
