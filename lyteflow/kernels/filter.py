"""All kernels that are involved with filtering their inputs

This module contains kernels that filter by columns or by index, as well as column and
index renaming, regex content filtering


"""

# Standard library imports
import warnings

# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement


class ColumnFilter(PipeElement):
    """A filter applied to columns selecting only the given columns of the DataFrame

    This is a subclass of PipeElement and therefore inherits all of its functions
    except for the transform function, which is overridden.

    Methods
    ------------------
    transform(x)
        Filters the columns on the given DataFrame

    """

    def __init__(self, columns, ignore_absent=False, **kwargs):
        """Constructor

        Arguments
        ------------------
        columns : list
            A list of columns that should be filtered for

        ignore_absent : bool
            If columns which are not found in the given columns should be ignored or
            should throw an error

        """
        PipeElement.__init__(self, **kwargs)
        self.columns = columns
        self.ignore_absent = ignore_absent

    def transform(self, x):
        """Filters the DataFrame for the given columns

        Arguments
        ------------------
        x : pd.DataFrame
            The DataFrame that should be filtered

        Returns
        ------------------
        x : pd.DataFrame
            Filtered DataFrame

        Raises
        ------------------
        KeyError
            When the ignore_absent is False and the set columns is not found in the given
            DataFrame

        """
        found_columns = x.columns.intersection(self.columns)
        if len(found_columns) != len(self.columns) and not self.ignore_absent:
            raise KeyError(
                f"{set(self.columns).difference(x.columns)} not found in the DataFrame"
            )
        else:
            return x.loc[:, found_columns]


class IndexFilter(PipeElement):
    """A filter applied to index selecting only the given index of the DataFrame

    This is a subclass of PipeElement and therefore inherits all of its functions
    except for the transform function, which is overridden.

    Methods
    ------------------
    transform(x)
        Filters the index on the given DataFrame

    """

    def __init__(self, index, ignore_absent=False, **kwargs):
        """Constructor

        Arguments
        ------------------
        index : list
            A list of indices that should be filtered for

        ignore_absent : bool
            If indices which are not found in the given index should be ignored or
            should throw an error

        """
        PipeElement.__init__(self, **kwargs)
        self.index = index
        self.ignore_absent = ignore_absent

    def transform(self, x):
        """Filters the DataFrame for the given index

        Arguments
        ------------------
        x : pd.DataFrame
            The DataFrame that should be filtered

        Returns
        ------------------
        x : pd.DataFrame
            Filtered DataFrame

        Raises
        ------------------
        KeyError
            When the ignore_absent is False and the set index is not found in the given
            DataFrame

        """
        found_indices = x.index.intersection(self.index)
        if len(found_indices) != len(self.index) and not self.ignore_absent:
            raise KeyError(
                f"{set(self.index).difference(x.index)} not found in the DataFrame"
            )
        else:
            return x.loc[found_indices, :]