"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports
import json

# Third party imports


# Local application imports
from kernels.io import Inlet, Outlet


class _Base:
    def __init__(self, **kwargs):
        """Abstract base class

        Base class for setting up uniform parameters. These parameters are used
        for all pipe connecting elements as well as the whole PipeSystem.

        Properties
        ------------------
        name : str (default: class name)
            The name of the class

        input_dimensions : tuple
            The dimension of the input

        output_dimensions : tuple
            The dimensino of the output

        input_columns : list
            The column names of the input if the input is a pandas DataFrame

        output_columns : list
            The column names of the output if the output is a pandas DataFrame

        """

        self.input_dimensions = None
        self.output_dimensions = None
        self.input_columns = None
        self.output_columns = None
        self.name = kwargs.get("name")
        if not name:
            name = self.__class__.__name__

    def flow(self, x):
        raise NotImplementedError


class PipeElement(_Base):
    def __init__(self, **kwargs):
        """A basic pipe element that is super class for all other pipe elements

        Arguments
        ------------------
        upstream : PipeElement/list of PipeElement
            The pipe element which is connected upstream, meaning the upstream
            element will flow data to this element

        downstream : PipeElement/list of PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

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

        Class Methods
        ------------------
        from_config()
            Creates PipeElement from config
        """

        self.upstream = kwargs.get("upstream")
        self.downstream = kwargs.get("downstream")
        if self.upstream is not None:
            kwargs.pop("upstream")
        if self.downstream is not None:
            kwargs.pop("downstream")

        _Base.__init__(self, **kwargs)

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
self.input_dimensions = None
        self.output_dimensions = None
        self.input_columns = None
        self.output_columns = None
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
        """
        # TODO: docstring
        """
        if self.upstream is None:
            self.upstream = upstream
        else:
            raise AttributeError("Upstream object already set")

    def attach_downstream(self, downstream):
        """
        # TODO: docstring
        """
        if self.downstream is None:
            self.downstream = downstream
        else:
            raise AttributeError("Downstream object already set")

    def to_config(self):
        down = (
            self.downstream if isinstance(self.downstream, list) else [self.downstream]
        )
        up = self.upstream if isinstance(self.upstream, list) else [self.upstream]

        config = {
            "class_name": self.__class__.__name__,
            "id": id(self),
            "upstream": [id(element) for element in up],
            "downstream": [id(element) for element in down],
            "attributes": self.__dict__,
        }
        return config

    def __call__(self, upstream):
        self.attach_upstream(upstream)
        upstream.attach_downstream(self)
        return self

    def __repr__(self):
        return f"{self.__class__.__name__}: {self.name}"


class PipeSystem(_Base):
    def __init__(self, inlets, outlets, **kwargs):
        _Base.__init__(self, **kwargs)

        for inlet in inlets:
            if not isinstance(inlet, Inlet):
                raise TypeError(f"{inlet} is not of type Inlet")

        for outlet in outlets:
            if not isinstance(outlet, Outlet):
                raise TypeError(f"{outlet} is not of type Outlet")

        self.inlets = inlets
        self.outlets = outlets

    def flow(self, inlet_data):
        """Initiates the flow of inlet_data to the PipeSystem Inlets


        """

        if self.validate_flow() == False:
            raise ValueError("Flow Validation Error")

        if len(self.inlets) != len(inlet_data):
            raise ValueError(
                f"Inlet data requires {len(self.inlets)} sources, "
                f"but only {len(inlet_data)} were given"
            )

        for i in range(len(self.inlets)):
            self.inlets[i].flow(inlet_data[i])

        if len(self.outlets) == 1:
            return self.outlets[0].output
        else:
            return [outlet.output for outlet in self.outlets]

    def validate_flow(self):
        marked = []
        ## TODO: Add check for loops

        def mark_recursive(element):
            if isinstance(element, list) is False:
                element = [element]

            for e in element:
                if isinstance(e.upstream, list) is False:
                    ups = [e.upstream]
                else:
                    ups = e.upstream

                if set(ups).issubset(set(self._marked)) or ups[0] is None:
                    self.marked.append(e)
                    if e.downstream is not None:
                        self.mark_recursive(e.downstream)

        mark_recursive(self.inlets)

        return set(self.inlets + self.outlets).issubset(set(marked))

    def to_config(self):
        if not validate_flow():
            raise ValueError("Invalid Pipesystems cannot be serialized")
        elements = {}

        def traverse(element):
            if not isinstance(element, list):
                element = [element]

            for e in element:
                if not e in elements and e not in self.inlets and e not in self.outlets:
                    elements.update({id(e): e.to_config()})
                traverse(e.downstream)

        element_dict = {id(e): e for e in elements}
        inlet_dict = {id(e): e.to_config() for e in self.inlets}
        outlet_dict = {id(e): e.to_config() for e in self.outlets}

        return {"inlet": inlet_dict, "outlet": outlet_dict, "elements": element_dict}

    def to_json(self, file_name="pipesystem.json"):
        with open(file_name, "w") as json_file:
            json.dump(self.to_config, json_file)

    @classmethod
    def from_config(cls, config):
        """TODO add description


        """
        return cls(**config)
