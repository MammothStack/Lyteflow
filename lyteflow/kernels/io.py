"""All PipeElements that handle In/Out flow of data in the PipeSystem

Inlet and Outlet classes are the only PipeElements that are recognized
and specified by the lyteflow.PipeSystem.

"""

# Standard library imports


# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement


class Inlet(PipeElement):
    """Inlet for data at beginning of the PipeSystem
    
    A subclass of the class PipeElement. Almost all functions
    of PipeElement are maintained except for the ability to 
    add an upstream PipeElement. This is due to Inlets being
    the beginning of the data flow and can therefore not have
    a Data source. The method attach_upstream is overridden
    from the superclass in order to achieve this.
    
    transform is overridden from the superclass as the inputs
    can be converted to a Numpy array or a pandas DataFrame.
    
    Methods
    ------------------    
    transform(x)
        Optional conversion of input into pandas.DataFrame

    attach_upstream(upstream)
        Throws an error if anything other than None is attached

    """

    def __init__(self, convert=True, **kwargs):
        """
        Arguments
        ------------------
        convert : bool
            If the input should be converted into a pandas.DataFrame
            
        """

        PipeElement.__init__(self, **kwargs)
        self.convert = convert

    def transform(self, x):
        """Optional conversion of input into pandas.DataFrame

        Arguments
        ------------------
        x : array/list
            The input that should be passed into the system

        Returns
        ------------------
        x : array/list/pandas.DataFrame
            Output which is optionally converted

        """
        if self.convert:
            x = np.asarray(x)
            self.input_dimensions = x.shape
            if len(x.shape) == 1 or len(x.shape) == 2:
                x = pd.DataFrame(x)
                self.input_columns = x.columns
        return x

    def attach_upstream(self, upstream):
        """Throws an error if anything other than None is attached"""
        if upstream is not None and upstream != tuple():
            raise AttributeError("Cannot attach an upstream element to an Inlet")


class Outlet(PipeElement):
    """Outlet for data at end of the PipeSystem

    A subclass of PipeElement dealing with the end of the PipeElement.
    All functions of the super class are maintained except for
    notably the ability to attach a PipeElement downstream of
    this Outlet. The method attach_downstream is overridden to
    achieve this.

    Methods
    ------------------
    attach_downstream(downstream)
        Attaches the given PipeElement as a downstream flow destination

    """

    def __init__(self, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.downstream = (None,)

    def attach_downstream(self, downstream):
        """Throws an error if anything other than None is attached"""
        if downstream is not None:
            raise AttributeError("Cannot attach a downstream element to an Outlet")
