"""Module for visualizing PipeElements and PipeSystems

Pydot and Graphviz are needed to be able to display the graphs

"""

# Standard library imports
import warnings

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
        pydot = None


# Local application imports
from lyteflow.construct import PipeSystem
from lyteflow.kernels.base import PipeElement