"""The base class for all subclassed PipeElements

This module contains the base class from which pipe elements should be subclassed

TODO: flesh out PipeElement

"""

# Standard library imports

# Third party imports

# Local application imports
from pypeflow.base import Base


class PipeElement(Base):

    def __init__(self, **kwargs):
        """A basic pipe element that is super class for all other pipe elements

        Arguments
        ------------------
        upstream : PipeElement/list of PipeBasicElement
            The pipe element which is connected upstream, meaning the upstream
            element will flow data to this element

        downstream : PipeElement/list of PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

        name : str
            The name that should be given to the PipeElement

        Methods
        ------------------
        transform(x)
            Method that is overwritten in each sub class

        flow(x)
            Method that is called when passing data to next PipeElement

        attach_upstream(upstream)
            Attaches the given PipeElement as an upstream flow source

        attach_downstream(downstream)
            Attaches the given PipeElement as a downstream flow destination

        to_config()
            Creates serializable PipeElement


        """

        self.upstream = kwargs.get("upstream")
        self.downstream = kwargs.get("downstream")
        if self.upstream is not None:
            kwargs.pop("upstream")
        if self.downstream is not None:
            kwargs.pop("downstream")

        Base.__init__(self, **kwargs)

    def transform(self, x):
        """Returns the given input"""
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
        except AttributeError:
            pass
        try:
            self.input_columns = x.columns
        except AttributeError:
            pass

        x = self.transform(x)

        self.output_dimensions = x.shape
        try:
            self.output_columns = x.columns
        except AttributeError:
            pass

        self.downstream.flow(x)

    def attach_upstream(self, upstream):
        """Attaches an upstream PipeElement

        Arguments
        ------------------
        upstream : PipeElement
            The PipeElement object that should be attached as an upstream element

        Raises
        ------------------
        AttributeError
            When an upstream element is already set

        """
        if self.upstream is None:
            self.upstream = upstream
        else:
            raise AttributeError("Upstream object already set")

    def attach_downstream(self, downstream):
        """Attaches a downstream PipeElement

        Arguments
        ------------------
        downstream : PipeElement
            The PipeElement object that should be attached as a downstream element

        Raises
        ------------------
        AttributeError
            When a downstream element is already set

        """
        if self.downstream is None:
            self.downstream = downstream
        else:
            raise AttributeError("Downstream object already set")

    def to_config(self):
        """Creates a dictionary of class variables

        Returns
        ------------------
        config : dict
            Dictionary of class variables

        """
        down = (
            self.downstream if isinstance(self.downstream, list) else [self.downstream]
        )
        up = self.upstream if isinstance(self.upstream, list) else [self.upstream]

        attributes = self.__dict__.copy()
        for i in ["upstream", "downstream", "input_dimensions", "output_dimensions", "input_columns", "output_columns"]:
            attributes.pop(i, None)

        config = {
            "class_name": self.__class__.__name__,
            "id": id(self),
            "upstream": [id(element) if element is not None else None for element in up],
            "downstream": [id(element) if element is not None else None for element in down],
            "attributes": attributes,
        }
        return config

    def __call__(self, upstream):
        """Attaches the given PipeElement in both directions

        Arguments
        ------------------
        upstream : PipeElement
            PipeElement to be attached upstream

        Returns
        ------------------
        self

        """
        self.attach_upstream(upstream)
        upstream.attach_downstream(self)
        return self

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.name}"
