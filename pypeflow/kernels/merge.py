"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports


# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from pypeflow.base import Basic


class _Merge(Basic):
    """# TODO: Add module title

    # TODO: add module description

    """

    def __init__(self, name):
        Basic.__init__(self, name)
        self.upstream = []

    def attach_upstream(self, *upstream):
        self.upstream + list(upstream)

    def __call__(self, *upstream):
        self.attach_upstream(*upstream)
        for element in upstream:
            element.attach_downstream(self)
