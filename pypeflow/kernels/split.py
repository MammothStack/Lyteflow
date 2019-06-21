"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports


# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from pypeflow.base import Basic


class _Split(Basic):
    """# TODO: Add module title

    # TODO: add module description

    """

    def __init__(self, name):
        Basic.__init__(self, name)
        self.downstream = []

    def attach_downstream(self, *downstream):
        self.downstream + list(downstream)


class Duplicator(_Split):
    def flow(self, x):
        pass
