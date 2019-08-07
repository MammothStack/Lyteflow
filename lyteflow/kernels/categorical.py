"""Module for all transformers that deal with data categorization

Module contents:
    Categorizer
    
This module helps with dealing with categorical data. The
Categorizer is used to convert categorical data into dense
or sparse matrices.

"""

# Standard library imports

# Third party imports
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement


class Categorizer(PipeElement):
    """Turns columns into dense or sparse categorical columns
    
    This class is a subclass of PipeElement and can therefore
    be used a PipeSystem. The only method this class overrides
    is the transform method. All other methods of the super 
    class PipeElement are maintained.

    Methods
    ------------------
    transform(x)
        Returns categorical data based on the columns
        
    """

    def __init__(
        self, columns=None, sparse=False, ignore_absent=False, keep=False, **kwargs
    ):
        """Constructor for the Categorizer class
        
        Arguments
        ------------------
        columns : list (default=None)
            The columns that should be converted to categorical data. If None then
            all given columns will be converted

        sparse : bool (default=False)
            If the resulting columns should be converted to spare or dense matrix

        ignore_absent : bool (default=False)
            If any given columns are not found in the given data should be ignored.
            Execution will halt if false

        keep : bool (default=False)
            If the columns that are categorized should be kept in the output
            
        Raises
        ------------------
        ValueError
            When column parameter is not given as a list or set as None
            
        """

        PipeElement.__init__(self, **kwargs)
        if isinstance(columns, list) or columns is None:
            self.columns = columns
        else:
            raise ValueError("Given columns has to be of type list or None")

        self.sparse = sparse
        self.ignore_absent = ignore_absent
        self.keep = keep

    def transform(self, x):
        """Returns categorical data based on the columns
        
        This method overrides its super method.

        Arguments
        ------------------
        x : pd.DataFrame
            The data that should be transformed into categorical data

        Returns
        ------------------
        x : pd.DataFrame
            Transformed data

        Raises
        ------------------
        KeyError
            When absent columns are not ignored and cannot be found in the given DataFrame

        """
        found_columns = []
        if self.columns is None:
            self.columns = list(x.columns)
        for col in self.columns:
            try:
                x[col] = x[col].astype("category")
                found_columns.append(col)
            except KeyError:
                if self.ignore_absent:
                    pass
                else:
                    raise KeyError(f"could not find {col} in DataFrame")
        if self.sparse:
            if self.keep:
                return pd.concat(x, pd.get_dummies(x[found_columns]), axis=1)
            else:
                return pd.get_dummies(x, columns=found_columns)
        else:
            x.loc[:, found_columns] = x.loc[:, found_columns].apply(
                lambda i: i.cat.codes
            )
            return x
