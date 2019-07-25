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
from lyteflow.base import Base
from lyteflow.kernels.io import Inlet, Outlet
from lyteflow.kernels.base import PipeElement, Requirement
from lyteflow.util import fetch_pipe_elements, connect_pipe_elements



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
        pass

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
        elements = fetch_pipe_elements(pipesystem=self, ignore_inlets=True, ignore_outlets=True)

        return {
            "inlets": [e.to_config() for e in self.inlets],
            "outlets": [e.to_config() for e in self.outlets],
            "elements": [e.to_config() for e in elements],
            "name": self.name,
        }

    def to_json(self, file_name="pipesystem.json"):
        """Saves a Json file with the given name

        Arguments
        ------------------
        file_name : str (default="pipesystem.json")
            The full file name where the json file should be written to

        :return:
        """
        with open(file_name, "w") as json_file:
            json.dump(self.to_config(), json_file, indent=4)

    @classmethod
    def from_config(cls, config):
        inlets = [Inlet.from_config(c,element_id=True) for c in config["inlet"]]
        outlets = [Outlet.from_config(c,element_id=True) for c in config["outlet"]]
        elements = [PipeElement.from_config(c,element_id=True) for c in config["elements"]
        
        connect_pipe_elements(inlets + outlets + elements)

        return cls(
            inlets=inlets,
            outlets=outlets,
            name=config["name"],
        )

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
