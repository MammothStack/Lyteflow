"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports

# Third party imports
import pandas as pd

# Local application imports
from pypeflow.kernels.base import PipeElement


class Normalizer(PipeElement):

    def __init__(self, column_relation=False, **kwargs):
        PipeElement.__init__(self, **kwargs)
        self.column_relation = column_relation

