"""The base class for all subclassed PipeElements

This module contains the base class from which pipe elements should be subclassed

TODO: flesh out PipeElement

"""

# Standard library imports
import importlib
import warnings

# Third party imports

# Local application imports
from lyteflow.base import Base


class PipeElement(Base):
    def __init__(self, **kwargs):
        """A basic pipe element that is super class for all other pipe elements

        Arguments
        ------------------
        upstream : PipeElement/tuple of PipeBasicElement
            The pipe element which is connected upstream, meaning the upstream
            element will flow data to this element

        downstream : PipeElement/tuple of PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

        requirements : Requirment/set of Requirement
            The Requirements of this PipeElement based on the values of other
            PipeElements
            
        func : function
            Transformer function used to transform the input. When this argument
            cannot be saved in the PipeElement.to_config() method

        name : str
            The name that should be given to the PipeElement

        Methods
        ------------------
        transform(x)
            Method that is overridden in each sub class

        flow(x)
            Method that is called when passing data to next PipeElement
            
        reset(x)
            Resets the PipeElement

        attach_upstream(upstream)
            Attaches the given PipeElement as an upstream flow source

        attach_downstream(downstream)
            Attaches the given PipeElement as a downstream flow destination

        to_config()
            Creates a dictionary of class variables

        from_config()
            Creates a PipeElement based on the given configuration

        """
        self.downstream = kwargs.get("downstream", tuple())
        self.upstream = kwargs.get("upstream", tuple())
        self.requirements = kwargs.get("requirements", set())
        self.func = kwargs.get("func", None)

        kwargs.pop("upstream", None)
        kwargs.pop("downstream", None)
        kwargs.pop("requirements", None)
        kwargs.pop("func", None)

        self._executed = False

        Base.__init__(self, **kwargs)

    @property
    def executed(self):
        return self._executed

    def can_execute(self):
        """If this PipeElement can be executed
        
        Both upstream PipeElements as well as Requirements are checked for
        their execution. If the those elements have not executed then this
        PipeElement cannot execute
        
        """
        for u in self.upstream:
            if not u.executed():
                return False
        
        for r in self.requirements:
            if not r.upstream.executed():
                return False

        return True

    def transform(self, x):
        """Returns the given input"""
        return self.func(x)

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
                requirement.pipe_element.__getattr__(requirement.attribute),
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
        
    def reset(self):
        """Resets the PipeElement"""
        self.input_columns, self.input_dimensions = None
        self.output_columns, self.output_dimensions = None
        self._executed = False

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
        if len(self.upstream) == 0:
            self.upstream = (upstream,)
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
        if len(self.downstream) == 0:
            self.downstream = (downstream,)
        else:
            raise AttributeError("Downstream object already set")

    def add_requirement(self, *requirements):
        """TODO: add docstring

        :param requirements:
        :return:
        """
        self.requirements = set(list(self.requirements) + list(requirements))

    def to_config(self):
        """Creates a dictionary of class variables

        Returns
        ------------------
        config : dict
            Dictionary of class variables

        """
        if not self.func is None:
            warnings.warn("User defined function passed to 'func' argument cannot be saved", RuntimeWarning)

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
            "func"
        ]:
            attributes.pop(i, None)

        config = {
            "class_name": self.__class__.__name__,
            "upstream": tuple([e.id for e in self.upstream]),
            "downstream": tuple([e.id for e in self.downstream]),
            "attributes": attributes,
            "requirements": set([r.to_config() for r in self.requirements]),
        }
        return config

    @staticmethod
    def from_config(config, element_id=False):
        """Creates a PipeElement based on the given configuration
        
        Arguments
        ------------------
        config : dict
            The configuration dictionary generated from a PipeElement's
            to_config() method
            
        element_id : bool
            If the PipeElement's upstream and downstream arguments should
            be set with their element.id instead of the actual object
            
        Returns
        ------------------
        PipeElement object
        
        """
        _cls = getattr(importlib.import_module("lyteflow"), config["class_name"])
        if element_id:
            return _cls(
                upstream=config[upstream], 
                downstream=config[downstream], 
                **config["attributes"]
            )
        else:
            return _cls(
                **config["attributes"]
            )

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

    def __init__(self, pipe_element, attribute, argument):
        self.pipe_element
        self.attribute = attribute
        self.argument = argument

    def to_config(self):
        return {
            "pipe_element": self.pipe_element.id, 
            "attribute": self.attribute, 
            "argument":self.argument
        }

    def __repr__(self):
        return (
            f"{self.__class__.__name__}:{self.pipe_element} ({self.attribute})-->"
            f"({self.argument})"
        )

    def __eq__(self, other):
        return(
			self.__class__ == other.__class__
			and self.pipe_element.id == other.pipe_element.id
			and self.attribute == other.attribute
			and self.argument == other.argument
		)
		
    def __hash__(self):
	    return hash((self.pipe_element.id, self.attribute, self.argument))