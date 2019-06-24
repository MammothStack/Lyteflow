"""Module for Construction of PipeSystems

The class PipeSystem is built as a container for the individual PipeElements. It handles
The inlets and outlets of the PipeElements and ensuring that the flow from all outlets
satisfy reachability in the PipeSystem's current configuration.

TODO: Add Verbosity to the flow process

"""

# Standard library imports
import json
import importlib

# Third party imports

# Local application imports
from pypeflow.base import Base
from pypeflow.kernels.io import Inlet, Outlet


class PipeSystem(Base):
    def __init__(self, inlets, outlets, **kwargs):
        """The container system to hold the PipeElements

        Arguments
        ------------------
        inlets : list
            The PipeElements that are data inlets for the PipeSystem

        outlets : list
            The PipeElements that are data outlets for the PipeSystem

        name : str
            The name of the PipeSystem

        Methods
        ------------------
        flow(x)
            Initiates the flow of inlet_data to the PipeSystem Inlets

        validate_flow()
            Ensures the flow data in the PipeSystem is valid

        to_config()
            Gives a configuration dictionary of class arguments

        to_json(file_name)

        Class Methods
        ------------------
        from_config()
            Creates PipeElement from config

        from_json()
            Creates PipeElement from json

        """
        Base.__init__(self, **kwargs)

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

        Arguments
        ------------------
        inlet_data : list (n sized for n Input PipeElements)
            A list of the data that should be passed to the Inlet PipeElements.
            Data should be given in order of the Inlet PipeElements specified during
            instantiation

        Returns
        ------------------
        output : list (n sized for the n Output PipeElements)
            Output of the Output PipeElements in the system

        Raises
        ------------------
        AttributeError
            When the PipeSystem's PipeElements are not set up correctly

        """

        if not self.validate_flow():
            raise AttributeError("Flow Validation Error")

        if len(self.inlets) != len(inlet_data):
            raise ValueError(
                f"Inlet data requires {len(self.inlets)} source(s), "
                f"but only {len(inlet_data)} were given"
            )

        for i in range(len(self.inlets)):
            self.inlets[i].flow(inlet_data[i])

        self.input_dimensions = [x.input_dimensions for x in self.inlets]
        self.input_columns = [x.input_columns for x in self.inlets]
        self.output_dimensions = [x.output_dimensions for x in self.outlets]
        self.output_columns = [x.output_columns for x in self.outlets]

        return [outlet.output for outlet in self.outlets]

    def validate_flow(self):
        """Ensures the flow data in the PipeSystem is valid

        Returns
        ------------------
        bool
            If the PipeSystem's flow is valid

        """

        marked = []

        def mark_recursive(element):
            if isinstance(element, list) is False:
                element = [element]

            for e in element:
                if isinstance(e.upstream, list) is False:
                    ups = [e.upstream]
                else:
                    ups = e.upstream

                if set(ups).issubset(set(marked)) or ups[0] is None:
                    marked.append(e)
                    if e.downstream is not None:
                        mark_recursive(e.downstream)

        mark_recursive(self.inlets)

        return set(self.inlets + self.outlets).issubset(set(marked))

    def to_config(self):
        """Gives a configuration dictionary of class arguments

        Returns
        ------------------
        config : dict
            A configuration dictionary of class arguments

        Raises
        ------------------
        AttributeError
            When the PipeSystem has an invalidly connected PipeElements

        """
        if not self.validate_flow():
            raise AttributeError("Invalid Pipesystems cannot be serialized")
        elements = []

        def traverse(element):
            if not isinstance(element, list):
                element = [element]

            for e in element:
                if e not in elements and e not in self.inlets and e not in self.outlets:
                    elements.append(e.to_config())
                traverse(e.downstream)

        traverse(self.inlets)
        in_conf = [e.to_config() for e in self.inlets]
        out_conf = [e.to_config() for e in self.outlets]

        return {"inlet": in_conf, "outlet": out_conf, "elements": elements, "name": self.name}

    def to_json(self, file_name="pipesystem.json"):
        """Saves a Json file with the given name

        Arguments
        ------------------
        file_name : str (default="pipesystem.json")
            The full file name where the json file should be written to

        :return:
        """
        with open(file_name, "w") as json_file:
            json.dump(self.to_config, json_file)

    @classmethod
    def from_config(cls, config):
        """Creates PipeElement from config


        """
        def construct_pipe_element(element):
            class_ = getattr(importlib.import_module("pypeflow"), element["class_name"])
            instance = class_(**element["upstream"], **element["downstream"], **element["attributes"])
            return instance

        def add_element_from_conf(configs, list_add, dict_add):
            for conf in configs:
                e = construct_pipe_element(conf)
                list_add.append(e)
                dict_add.update({conf["id"]: e})

        inlets, outlets, elements = [], [], []
        inlet_index, outlet_index, element_index = {}, {}, {}

        add_element_from_conf(config["inlet"], list_add=inlets, dict_add=inlet_index)
        add_element_from_conf(config["outlet"], list_add=outlets, dict_add=outlet_index)
        add_element_from_conf(config["elements"], list_add=elements, dict_add=element_index)

        for i in inlets:
            if i.downstream[0] in element_index.keys():
                i.downstream = element_index[i.downstream[0]]
            elif i.downstream[0] in outlet_index.keys():
                i.downstream = outlet_index[i.downstream[0]]

        for o in outlets:
            if o.upstream[0] in element_index.keys():
                o.upstream = element_index[o.upstream[0]]
            elif o.upstream[0] in inlet_index.keys():
                o.upstream = inlet_index[o.upstream[0]]

        for e in elements:
            down, up = [], []
            for i in e.downstream:
                if i in element_index.keys():
                    down.append(element_index[i])
                elif i in outlet_index.keys():
                    down.append(outlet_index[i])

            for i in e.upstream:
                if i in element_index.keys():
                    up.append(element_index[i])
                elif i in inlet_index.keys():
                    up.append(inlet_index[i])

            if len(down) == 1:
                down = down[0]
            if len(up) == 1:
                up = up[0]

            e.downstream = down
            e.upstream = up

        return cls(inlets=inlets, outlets=outlets, name=config["name"])

    @classmethod
    def from_json(cls, json_file_name):
        """Creates PipeElement from json

        :param json_file_name:
        :return:
        """
        file = open(json_file_name)
        json_str = file.read()
        config = json.loads(json_str)
        return cls.from_config(config)