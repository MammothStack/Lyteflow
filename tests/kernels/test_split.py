# Third party imports
import pytest
import numpy as np

# Local imports
from lyteflow.kernels import *


@pytest.fixture()
def duplicator():
    return Duplicator(name="Dupe")


class TestDuplicator:
    pass
