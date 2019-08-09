"""Module for visualizing PipeElements and PipeSystems

Pydot and Graphviz are needed to be able to display the graphs

"""

# Standard library imports
import warnings
import os

# Third party imports
try:
    # pydot-ng is a fork of pydot that is better maintained.
    import pydot_ng as pydot
except ImportError:
    # pydotplus is an improved version of pydot
    try:
        import pydotplus as pydot
    except ImportError:
    # Fall back on pydot if necessary.
        try:
            import pydot
        except ImportError:
            raise ImportError("pydot_ng, pydotplus, or pydot need to be installed")

# Local application imports
from lyteflow.construct import PipeSystem
from lyteflow.kernels.base import PipeElement
from lyteflow.kernels.io import Inlet, Outlet

def _pipe_system_to_dot(pipe_system):
    dot = pydot.Dot()
    dot.set("rankdir", "TB")
    dot.set("concentrate", True)
    dot.set_node_defaults(shape="record", style="rounded", fontname="Trebuchet MS", fontsize=10)
    dot.set_edge_defaults(fontname="Trebuchet MS", fontsize=9)
    
    # Nodes
    for element in pipe_system.all_elements:
        element_class = element.__class__.__name__
        in_dim = str(element.input_dimensions)
        out_dim = str(element.output_dimensions)
        label = "{ In: %s | %s : %s | Out: %s }" % (in_dim, element_class, element.name, out_dim)
        
        if isinstance(element, Inlet) or isinstance(element, Outlet):
            node = pydot.Node(str(element.id), label=label, style="filled, rounded", )
        else:
            node = pydot.Node(str(element.id), label=label)
        dot.add_node(node)
    
    # Edges
    for element in pipe_system.all_elements:
        for down in element.downstream:
            dot.add_edge(pydot.Edge(str(element.id), str(down.id)))
        for req in element.requirements:
            dot.add_edge(pydot.Edge(str(req.pipe_element.id), str(element.id), style="dashed", label="Requirement"))
    
    return dot


def plot_pipe_system(pipe_system, file_name="pipe_system.png"):
    """Graphs the given PipeSystem's PipeElement Graph
    
    Arguments
    ------------------
    pipe_system : PipeSystem
        The PipeSystem that should be graphed
        
    file_name : str
        The name of the file that is produced
    
    
    """
    
    file, extension = os.path.splitext(file_name)
    extension = "png" if extension is None else extension[1:]
    dot = _pipe_system_to_dot(pipe_system)
    dot.write(file_name, format=extension)
    
    # Display if in Notebook
    try:
        from IPython import display
        return display.Image(filename=file_name)
    except ImportError:
        pass
    