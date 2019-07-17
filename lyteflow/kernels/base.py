"""The base class for all subclassed PipeElements

This module contains the base class from which pipe elements should be subclassed

TODO: flesh out PipeElement

"""

# Standard library imports
import importlib

# Third party imports

# Local application imports
from lyteflow.base import Base


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

        TODO: add requiremnts

        name : str
            The name that should be given to the PipeElement

        Methods
        ------------------
        transform(x)
            Method that is overridden in each sub class

        flow(x)
            Method that is called when passing data to next PipeElement

        attach_upstream(upstream)
            Attaches the given PipeElement as an upstream flow source

        attach_downstream(downstream)
            Attaches the given PipeElement as a downstream flow destination

        to_config()
            Creates serializable PipeElement


        """
        self.downstream = kwargs.get("downstream")
        self.upstream = kwargs.get("upstream")
        self.requirements = kwargs.get("requirements")

        kwargs.pop("upstream", None)
        kwargs.pop("downstream", None)
        kwargs.pop("requirements", None)

        self._executed = False
        # self._output = None

        Base.__init__(self, **kwargs)

    @property
    def executed(self):
        return self._executed

    def can_execute(self):
        try:
            for u in self.upstream:
                if not u.executed():
                    return False
        except AttributeError:
            if not self.upstream.executed():
                return False

        for r in self.requirements:
            if not r.upstream.executed():
                return False

        return True

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
        if not self.can_execute():
            raise AttributeError("Upstream elements or requirements are not executed")
        for requirement in self.requirements:
            self.__setattr__(
                requirement.argument,
                requirement.upstream.__getattr__(requirement.attribute),
            )
        try:
            self.input_dimensions = x.shape
            self.input_columns = x.columns
        except AttributeError:
            pass

        if x is not None:
            x = self.transform(x)

        try:
            self.output_dimensions = x.shape
            self.output_columns = x.columns
        except AttributeError:
            pass
        self._executed = True
        return self.downstream, x

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

    def add_requirement(self, *requirements):
        """

        :param requirements:
        :return:
        """
        self.requirements = self.requirements + set(requirements)

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
        for i in [
            "upstream",
            "downstream",
            "input_dimensions",
            "output_dimensions",
            "input_columns",
            "output_columns",
            "_executed",
            "requirements",
        ]:
            attributes.pop(i, None)

        if self.requirements is None:
            req = [None]
        else:
            req = [r.to_config() for r in self.requirements]

        config = {
            "class_name": self.__class__.__name__,
            "upstream": [None if element is None else element.id for element in up],
            "downstream": [None if element is None else element.id for element in down],
            "attributes": attributes,
            "requirements": req,
        }
        return config

    @staticmethod
    def from_config(config, upstream=None, downstream=None):
        def _verify_connection(elements, updown="upstream"):
            if not isinstance(elements, list):
                elements = [elements]
            for element in elements:
                if element.id not in config[updown]:
                    raise KeyError(f"{element.id} not found in config[{updown}]")

        _cls = getattr(importlib.import_module("lyteflow"), config["class_name"])

        if upstream is not None:
            _verify_connection(upstream, "upstream")
        if downstream is not None:
            _verify_connection(downstream, "downstream")

        return _cls(upstream=upstream, downstream=downstream, **config["attributes"])

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

    def __eq__(self, other):
        if isinstance(other, PipeElement):
            return self.__class__ == other.__class__ and self.to_config() == other.to_config()
        else:
            return False
		
		

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.name}::{self.id}"


class Requirement:
    """
    # TODO: add docstring
    """

    def __init__(self, upstream, downstream, attribute, argument):
        if upstream is None:
            raise ValueError("upstream cannot be None")

        if downstream is None:
            raise ValueError("downstream cannot be None")

        self.upstream = upstream
        self.downstream = downstream
        self.attribute = attribute
        self.argument = argument

    def to_config(self):
        d = self.__dict__.copy()
        d.update({"upstream": d["upstream"].id, "downstream": d["downstream"].id})
        return d

    def __repr__(self):
        return (
            f"{self.__class__.__name__}:{self.upstream} --({self.attribute})-"
            f"({self.argument})--> {self.downstream}"
        )

    def __eq__(self, other):
        return(
			self.__class__ == other.__class__
			and self.upstream.id == other.upstream.id
			and self.downstream.id == other.downstream.id
			and self.attribute == other.attribute
			and self.argument == other.argument
		)


