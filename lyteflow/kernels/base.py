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
    """A basic pipe element that is super class for all other pipe elements
    
    A PipeElement is the basic building block of all other data transformers.
    It can be attached to other PipeElements either as an upstream or a
    downstream element. Data always flows from upstream to downstream.
    
    Once the right stream configuration is set the PipeElement can be
    executed or "flowed". When the PipeElement flows data transformations
    are executed depending on the transform method. This method is overridden
    when the PipeElement is subclassed. After execution the transformed data
    is returned including the downstream pipe elements to which this data
    should go.
    
    PipeElements can have a Requirement based on another PipeElement. This
    means if the transform function needs an argument based on another
    PipeElement's attributes after execution, then the Requirement will
    ensure that the right arguments and attributes are set. This also
    affects the execution order of all the connected PipeElements, as
    Requirements often take attributes that are only present after
    execution.
    
    Methods
    ------------------
    can_execute()
        If this PipeElement can be executed
        
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
        
    add_requirement(*requirements)
        Adds a requirement to the PipeElement
    
    validate_stream()
        Validates that upstream, downstream, and requirements exist
        
    to_config()
        Creates a dictionary of class variables

    from_config()
        Creates a PipeElement based on the given configuration
        
    """

    def __init__(self, **kwargs):
        """Constructor for PipeElement

        Arguments
        ------------------
        upstream : PipeElement/tuple of PipeBasicElement
            The pipe element which is connected upstream, meaning the upstream
            element will flow data to this element

        downstream : PipeElement/tuple of PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

        requirements : Requirement/set of Requirement
            The Requirements of this PipeElement based on the values of other
            PipeElements
            
        func : function
            Transformer function used to transform the input. When this argument
            cannot be saved in the PipeElement.to_config() method
            
        """
        self.downstream = kwargs.get("downstream", tuple())
        self.upstream = kwargs.get("upstream", tuple())
        self.requirements = kwargs.get("requirements", set())
        self.func = kwargs.get("func", None)
        self._unconfigured_downstream = kwargs.get("unconfigured_downstream", None)
        self._unconfigured_upstream = kwargs.get("unconfigured_upstream", None)
        self._unconfigured_requirements = kwargs.get("unconfigured_requirements", None)

        kwargs.pop("upstream", None)
        kwargs.pop("downstream", None)
        kwargs.pop("requirements", None)
        kwargs.pop("func", None)
        kwargs.pop("unconfigured_requirements", None)
        kwargs.pop("unconfigured_upstream", None)
        kwargs.pop("unconfigured_downstream", None)

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
        return x if self.func is None else self.func(x)

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
        x : FlowData
            The output of the data transformation that should be passed downstream

        """
        if not self.can_execute():
            raise AttributeError("Upstream elements or requirements are not executed")

        if x.to_element != self:
            raise ValueError(f"{x.to_element} does not equal {self}")

        for requirement in self.requirements:
            self.__setattr__(
                requirement.argument,
                requirement.pipe_element.__getattr__(requirement.attribute),
            )
        try:
            self.input_dimensions = x.data.shape
            self.input_columns = x.data.columns
        except AttributeError:
            pass

        if x is not None:
            x = self.transform(x.data)

        try:
            self.output_dimensions = x.shape
            self.output_columns = x.columns
        except AttributeError:
            pass

        self._executed = True

        return [FlowData(self, x, self.downstream[0])]

    def reset(self):
        """Resets the PipeElement"""
        self.input_columns, self.input_dimensions = None, None
        self.output_columns, self.output_dimensions = None, None
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
            raise AttributeError("Only one Upstream object may be set")

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
            raise AttributeError("Only one Downstream object may be set")

    def add_requirement(self, *requirements):
        """Adds a requirement to the PipeElement

        Arguments
        ------------------
        *requirements : Requirement
            The requirement(s) that should be added
            
        """
        self.requirements = self.requirements.union(set(requirements))

    def reconfigure(self, *pipe_element):
        """Reconfigures all unconfigured PipeElements and Requirements from config

        When a PipeElement is created through from_config method,
        the PipeElements and the Requirements that come from that configuration
        dictionary are also just configuration dictionaries, and must therefore also be
        converted back into the actual PipeElement and Requirement objects. The config
        dictionaries are iterated through and matched up with the given PipeElements in
        order to match their ID number. Through this method the proper object reference
        are re-established.

        Arguments
        ------------------
        *pipe_element : PipeElement
            The PipeElement(s) that is referenced by ID in the configuration file of
            each unconfigured Requirement

        Raises
        ------------------
        ValueError
            When the given PipeElements do not sufficiently manage to re-establish all
            the references

        """
        upstream = [None] * len(self._unconfigured_upstream)
        downstream = [None] * len(self._unconfigured_downstream)
        requirements = []

        for e in pipe_element:
            for i in range(len(self._unconfigured_upstream)):
                if self._unconfigured_upstream[i] == e.id:
                    upstream[i] = e

            for i in range(len(self._unconfigured_downstream)):
                if self._unconfigured_downstream[i] == e.id:
                    downstream[i] = e

            for req in self._unconfigured_requirements:
                if req["pipe_element"] == e.id:
                    requirements.append(Requirement.from_config(req, e))

        if len(upstream) != len(self._unconfigured_upstream):
            raise ValueError(
                "Given Pipe Elements could not cover all upstream elements"
            )

        elif len(downstream) != len(self._unconfigured_downstream):
            raise ValueError(
                "Given Pipe Elements could not cover all downstream " "elements"
            )

        elif len(set(requirements)) != len(self._unconfigured_requirements):
            raise ValueError("Not all Requirements could be created")

        else:
            self.downstream = tuple(downstream)
            self.upstream = tuple(upstream)
            self.requirements = set(requirements)
            self._unconfigured_downstream = None
            self._unconfigured_upstream = None
            self._unconfigured_requirements = None

    def to_config(self):
        """Creates a dictionary of class variables

        Returns
        ------------------
        config : dict
            Dictionary of class variables

        """
        if self.func is not None:
            warnings.warn(
                "User defined function passed to 'func' argument cannot be saved",
                RuntimeWarning,
            )

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
            "func",
        ]:
            attributes.pop(i, None)

        config = {
            "class_name": self.__class__.__name__,
            "unconfigured_upstream": [e.id for e in self.upstream],
            "unconfigured_downstream": [e.id for e in self.downstream],
            "attributes": attributes,
            "unconfigured_requirements": [r.to_config() for r in self.requirements],
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
                unconfigured_upstream=config["unconfigured_upstream"],
                unconfigured_downstream=config["unconfigured_downstream"],
                unconfigured_requirements=config["unconfigured_requirements"],
                **config["attributes"],
            )
        else:
            return _cls(**config["attributes"])

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
        return f"{self.__class__.__name__}: {self.name}::{self.id}"


class Requirement:
    """Data connection between PipeElements
    
    A Requirement can used to connect the meta data of one PipeElement to the required
    arguments of another. This means that certain results of a transformer can be
    utilized by other PipeElements. This is contingent on the PipeElements having
    executed first so the meta data is actually available.
    
    Methods
    ------------------
    to_config()
        Converts the Requirement into configuration dictionary
        
    from_config(config, pipe_element)
        Converts the configuration dictionary into a Requirement
        
        
    """

    def __init__(self, pipe_element, attribute, argument):
        """Constructor for the Requirement class
        
        Arguments
        ------------------
        pipe_element : PipeElement
            The PipeElement that has the required attribute
            
        attribute : str
            The name of the required attribute
        
        argument : str
            The name of the argument which should be replaced
            by the attribute
        
        """
        self.pipe_element = pipe_element
        self.attribute = attribute
        self.argument = argument

    def to_config(self):
        """Converts the Requirement into configuration dictionary
        
        Returns
        ------------------
        config : dict
        
        """
        return {
            "pipe_element": self.pipe_element.id,
            "attribute": self.attribute,
            "argument": self.argument,
        }

    @classmethod
    def from_config(cls, config, pipe_element):
        """Converts the configuration dictionary into a Requirement
        
        Arguments
        ------------------
        config : dict
            The config that should be converted to a Requirement
            
        pipe_element : PipeElement
            The PipeElement that is specified in the config
        
        Returns
        ------------------
        r : Requirement
            The Requirement based on the config data

        """
        if config["pipe_element"] == pipe_element.id:
            config.update({"pipe_element": pipe_element})
            return cls(**config)
        else:
            raise ValueError(f"{pipe_element} is not equal to the given configuration")

    def __repr__(self):
        return (
            f"{self.__class__.__name__}:{self.pipe_element} ({self.attribute})-->"
            f"({self.argument})"
        )

    def __eq__(self, other):
        return (
            self.__class__ == other.__class__
            and self.pipe_element.id == other.pipe_element.id
            and self.attribute == other.attribute
            and self.argument == other.argument
        )

    def __hash__(self):
        return hash((self.pipe_element.id, self.attribute, self.argument))


class FlowData:
    def __init__(self, from_element, data, to_element):
        self.from_element = from_element
        self.data = data
        self.to_element = to_element
