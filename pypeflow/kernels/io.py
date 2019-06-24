"""All PipeElements that handle In/Out flow of data in the PipeSystem

Inlet and Outlet classes are the only PipeElements that are recognized
and specified by the pypeflow.PipeSystem.

"""

# Standard library imports


# Third party imports
import numpy as np
import pandas as pd

# import pandas as pd

# Local application imports
from pypeflow.kernels.base import PipeElement


class Inlet(PipeElement):

    def __init__(self, convert=True, **kwargs):
        """Inlet for data at beginning of the PipeSystem

        Arguments
        ------------------
        convert : bool
            If the input should be converted into a pandas.DataFrame

        upstream
            Only None can be attached as an upstream element

        downstream : PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

        name : str
            The name that should be given to the PipeElement

        Methods
        ------------------
        transform(x)
            Optional conversion of input into pandas.DataFrame

        flow(x)
            Method that is called when passing data to next PipeElement

        attach_upstream(upstream)
            Throws an error if anything other than None is attached

        attach_downstream(downstream)
            Attaches the given PipeElement as a downstream flow destination

        to_config()
            Creates serializable PipeElement

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
            if isinstance(x, list):
                x = np.asarray(x)
                self.input_dimensions = x.shape
            if len(x.shape) == 1 or len(x.shape) == 2:
                x = pd.DataFrame(x)
                self.input_columns = x.columns
        return x

    def attach_upstream(self, upstream):
        """Throws an error if anything other than None is attached"""
        if upstream is not None:
            raise AttributeError("Cannot attach an upstream element to an Inlet")


class Outlet(PipeElement):

    def __init__(self, **kwargs):
        """Outlet for data at end of the PipeSystem

        Arguments
        ------------------
        upstream : None
            Only None can be attached as an upstream element

        downstream : PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

        name : str
            The name that should be given to the PipeElement

        Attributes
        ------------------
        output : array-like (numpy.array/pandas.DataFrame)
            The output at the end of the PipeSystem

        Methods
        ------------------
        transform(x)
            Stores output in class variable

        flow(x)
            Method that is called when passing data to next PipeElement

        attach_upstream(upstream)
            Throws an error if anything other than None is attached

        attach_downstream(downstream)
            Attaches the given PipeElement as a downstream flow destination

        to_config()
            Creates serializable PipeElement


        """
        PipeElement.__init__(self, **kwargs)
        self.output = None

    def attach_downstream(self, downstream):
        """Throws an error if anything other than None is attached"""
        if downstream is not None:
            raise AttributeError("Cannot attach a downstream element to an Outlet")

    def transform(self, x):
        """Sets output Variable to the result

        """
        self.output = x
        return x

    def flow(self, x):
        """Receives flow from upstream, transforms and flows data to downstream elements

        Receives the flow x. The input values such as shape and columns, in case
        the input data is a pandas DataFrame.

        Arguments
        ------------------
        x : numpy array/pandas DataFrame
            The input flow that should be transformed and passed downstream

        """
        try:
            self.input_dimensions = x.shape
            self.input_columns = x.columns
        except AttributeError:
            pass

        x = self.transform(x)

        try:
            self.output_dimensions = x.shape
            self.output_columns = x.columns
        except AttributeError:
            pass

    def to_config(self):
        config = super().to_config()
        config["attributes"].pop("output")
        return config
