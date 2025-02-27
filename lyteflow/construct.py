"""Module for Construction of PipeSystems

The class PipeSystem is built as a container for the individual PipeElements. It handles
The inlets and outlets of the PipeElements and ensuring that the flow from all outlets
satisfy reachability in the PipeSystem's current configuration.

"""

# Standard library imports
import json

# Third party imports
from tqdm import tqdm

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
        self.elements_non_io = fetch_pipe_elements(
            self, ignore_inlets=True, ignore_outlets=True
        )
        self.all_elements = inlets + outlets + self.elements_non_io
        self.verbose = verbose
        self.execution_sequence = PTGraph(self).get_execution_sequence()

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

        data_hold = {e: [] for e in self.all_elements}
        output = {}

        for i in range(len(self.inlets)):
            data_hold[self.inlets[i]].append(
                FlowData(data=inlet_data[i], to_element=self.inlets[i])
            )

        if self.verbose:
            pbar = tqdm(self.execution_sequence)
            for pipe_element in pbar:
                pbar.set_description(f"Flowing {pipe_element.name}")
                _execution(pipe_element, data_hold, output)
            pbar.close()
        else:
            for pipe_element in self.execution_sequence:
                _execution(pipe_element, data_hold, output)

        self.input_dimensions = [x.input_dimensions for x in self.inlets]
        self.input_columns = [x.input_columns for x in self.inlets]
        self.output_dimensions = [x.output_dimensions for x in self.outlets]
        self.output_columns = [x.output_columns for x in self.outlets]
        self._executed = True

        return [output[outlet].data for outlet in self.outlets]

    def reset(self):
        """Resets all the PipeElements and itself
        
        
        
        """
        super().reset()
        for element in self.all_elements:
            element.reset()

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

        return {
            "inlets": [e.to_config() for e in self.inlets],
            "outlets": [e.to_config() for e in self.outlets],
            "elements": [e.to_config() for e in self.elements_non_io],
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
            Configured PipeSystem
        
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

    @staticmethod
    def add(*pipe_system):
        """Adds the given PipeSystems into a single concurrent PipeSystem

        The given PipeSystems are added by combining their inlets and outlets into two
        combined lists and feeding that into a single PipeSystem. This results in a
        single PipeSystem with the given PipeSystems as concurrent elements

        Arguments
        ------------------
        *pipe_system : PipeSystem
            The PipeSystems that should be added together

        Returns
        ------------------
        ps : PipeSystem
            The single PipeSystem into which the given PipeSystems were added

        Raises
        ------------------
        TypeError
            When something else other than a PipeSystem is trying to be added

        """
        for a in pipe_system:
            if not isinstance(a, PipeSystem):
                raise TypeError(f"Cannot extend PipeSystems with type {type(a)}")

        if len(pipe_system) == 1:
            return pipe_system[0]

        all_inlets = [
            inlet for pipe_system in pipe_system for inlet in pipe_system.inlets
        ]
        all_outlets = [
            outlet for pipe_system in pipe_system for outlet in pipe_system.outlets
        ]
        name = ": ".join([pipe_system.name for pipe_system in pipe_system])
        return PipeSystem(inlets=all_inlets, outlets=all_outlets, name=name)

    @staticmethod
    def concatenate(*pipe_system):
        """Concatenates the given PipeSystems into a single Sequential PipeSystem

        The PipeSystems are concatenated together by removing their inlets and outlets
        and connecting them via a PipeElement. This expects that the amount of Outlets
        matches the amount of Inlets of the PipeSystem is supposed to connect to. If a
        discrepancy here is found an Exception is raised.

        Arguments
        ------------------
        *pipe_system : PipeSystem
            The PipeSystems that should be concatenated together

        Returns
        ------------------
        ps : PipeSystem
            The single PipeSystem with the concatenated PipeSystem

        Raises
        ------------------
        TypeError
            When something else other than a PipeSystem is trying to be added

        AttributeError
            When the length of the outlets of a PipeSystem do not match the following
            PipeSystem's outlets

        """

        for e in pipe_system:
            if not isinstance(e, PipeSystem):
                raise TypeError(f"Cannot extend PipeSystems with type {type(e)}")

        if len(pipe_system) == 1:
            return pipe_system[0]

        for i, ps in enumerate(pipe_system):
            if i > 0:
                if len(pipe_system[i - 1].outlets) != len(pipe_system[i].inlets):
                    raise AttributeError(
                        f"{pipe_system[i-1]} outlets do not match amount of "
                        f"{pipe_system[i]} inlets"
                    )

        inlets = pipe_system[0].inlets
        outlets = pipe_system[-1].outlets
        name = ""

        for i, ps in enumerate(pipe_system):
            if i > 0:
                for outlet, inlet in zip(
                    pipe_system[i - 1].outlets, pipe_system[i].inlets
                ):
                    up = outlet.upstream[0]
                    down = inlet.downstream[0]
                    up.detach_downstream()
                    down.detach_upstream()

                    down(PipeElement(name=outlet.name + inlet.name)(up))

            name += ps.name

        return PipeSystem(inlets=inlets, outlets=outlets, name=name)

    def __len__(self):
        return len(self.all_elements)

    def __contains__(self, element):
        return element in self.all_elements

    def __add__(self, other):
        return self.add(self, other)

    def __mul__(self, other):
        return self.concatenate(self, other)
