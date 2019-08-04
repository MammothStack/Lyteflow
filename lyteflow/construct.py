"""Module for Construction of PipeSystems

The class PipeSystem is built as a container for the individual PipeElements. It handles
The inlets and outlets of the PipeElements and ensuring that the flow from all outlets
satisfy reachability in the PipeSystem's current configuration.

"""

# Standard library imports
import json
import warnings
import sys

# Third party imports
try:
    from tqdm import tqdm
except ImportError:
    pass

# Local application imports
from lyteflow.base import Base
from lyteflow.kernels.io import Inlet, Outlet
from lyteflow.kernels.base import PipeElement, FlowData
from lyteflow.util import fetch_pipe_elements, PTGraph


class PipeSystem(Base):
    """The container system to hold the PipeElements
    
    A PipeSystem is responsible for the correct sequential execution of all the connected
    PipeElements. The PipeSystem needs to be initialized with the inlets and outlets of
    the system. This is to ensure the PipeSystem has a reference what the inputs and
    outputs are, which is important in calculating the reachability and execution
    sequence of the system.

    Attributes
    ------------------
    execution_sequence : list
        The list of PipeElements ordered in their recommended execution

    verbose : bool
        If the flow execution is verbose
    
    Methods
    ------------------
    flow(x)
        Initiates the flow of inlet_data to the PipeSystem Inlets

    to_config()
        Gives a configuration dictionary of class arguments

    to_json(file_name)
        Saves a Json file with the given name
    
    Class Methods
    ------------------
    from_config()
        Creates PipeElement from config

    from_json()
        Creates PipeElement from json
    
    """

    def __init__(self, inlets, outlets, verbose=False, **kwargs):
        """Constructor of the PipeSystem

        Arguments
        ------------------
        inlets : list
            The PipeElements that are data inlets for the PipeSystem

        outlets : list
            The PipeElements that are data outlets for the PipeSystem

        verbose : bool
            If flow execution should be verbose

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
        if verbose and "tqdm" not in sys.modules:
            warnings.warn("tqdm should be imported for verbose mode", ImportWarning)
            verbose = False
        self.verbose = verbose
        self.execution_sequence = PTGraph.get_execution_sequence_(self)

    def flow(self, *inlet_data):
        """Initiates the flow of inlet_data to the PipeSystem Inlets

        Arguments
        ------------------
        *inlet_data : numpy.array/pandas.DataFrame
            A tuple of the data that should be passed to the Inlet PipeElements.
            Data should be given in order of the Inlet PipeElements specified during
            instantiation

        Returns
        ------------------
        output : list (n sized for the n Output PipeElements)
            Output of the Output PipeElements in the system

        Raises
        ------------------
        AttributeError
            No execution sequence possible

        ValueError
            Inlets does not match the amount of data given

        """

        def _execution(pipe_element_, data_hold_, output_):
            flow_data = pipe_element_.flow(*data_hold_[pipe_element_])
            for fd in flow_data:
                if fd.to_element is not None:
                    data_hold_[fd.to_element].append(fd)
                else:
                    output_.update({fd.from_element: fd})

        if self.execution_sequence is None:
            raise AttributeError(f"No execution sequence possible")

        if len(self.inlets) != len(inlet_data):
            raise ValueError(
                f"Inlet data requires {len(self.inlets)} source(s), "
                f"but only {len(inlet_data)} were given"
            )

        data_hold = {e: [] for e in fetch_pipe_elements(self)}
        output = {}

        for i in range(len(self.inlets)):
            data_hold[self.inlets[i]].append(
                FlowData(data=inlet_data[i], to_element=self.inlets[i])
            )

        if self.verbose:
            for pipe_element in tqdm(self.execution_sequence):
                _execution(pipe_element, data_hold, output)
        else:
            for pipe_element in self.execution_sequence:
                _execution(pipe_element, data_hold, output)

        self.input_dimensions = [x.input_dimensions for x in self.inlets]
        self.input_columns = [x.input_columns for x in self.inlets]
        self.output_dimensions = [x.output_dimensions for x in self.outlets]
        self.output_columns = [x.output_columns for x in self.outlets]

        return [output[outlet].data for outlet in self.outlets]

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

        elements = fetch_pipe_elements(
            pipesystem=self, ignore_inlets=True, ignore_outlets=True
        )

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
            
        """
        with open(file_name, "w") as json_file:
            json.dump(self.to_config(), json_file, indent=4)

    @classmethod
    def from_config(cls, config):
        """Creates a PipeSystem from Pipesystem configuration
        
        Arguments
        ------------------
        config : dict
            The configuration that should be converted into a
            PipeSystem
            
        Returns
        ------------------
        PipeSystem
        
        """
        inlets = [Inlet.from_config(c, element_id=True) for c in config["inlets"]]
        outlets = [Outlet.from_config(c, element_id=True) for c in config["outlets"]]
        elements = [
            PipeElement.from_config(c, element_id=True) for c in config["elements"]
        ]
        _all = inlets + outlets + elements

        for e in _all:
            e.reconfigure(*_all)

        return cls(inlets=inlets, outlets=outlets, name=config["name"])

    @classmethod
    def from_json(cls, json_file_name):
        """Creates PipeSystem from json

        Arguments
        ------------------
        json_file_name : str
            The location of the file name of the json file
        
        Returns
        ------------------
        PipeSystem
        
        """
        file = open(json_file_name)
        json_str = file.read()
        config = json.loads(json_str)
        return cls.from_config(config)
