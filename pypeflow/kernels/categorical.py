"""# TODO: Add module title

# TODO: add module description

"""

# Standard library imports

# Third party imports
import pandas as pd

# Local application imports
from pypeflow.kernels.base import PipeElement


class Categorizer(PipeElement):
    def __init__(
        self,
        columns=None,
        sparse=False,
        absent_ignore=False,
        keep=False,
        **kwargs
    ):
        """Inlet for data at beginning of the PipeSystem

        Arguments
        ------------------
        columns : list (default=None)
            The columns that should be converted to categorical data. If None then
            all given columns will be converted

        sparse : bool (default=False)
            If the resulting columns should be converted to spare or dense matrix

        absent_ignore : bool (default=False)
            If any given columns are not found in the given data should be ignored.
            Execution will halt if false

        keep : bool (default=False)
            If the columns that are categorized should be kept in the output

        upstream : PipeElement
            The pipe element which is connected upstream, meaning the upstream
            element will flow data to this element

        downstream : PipeElement
            The pipe element which is connected downstream, meaning this pipe
            element will flow data to the downstream element

        name : str
            The name that should be given to the PipeElement

        Methods
        ------------------
        transform(x)
            Optional conversion of input into pandas.DataFrame

        flow(x)
            Method that is called when passing data to next PipeElement

        attach_upstream(upstream)
            Throws an error if anything other than None is attached

        attach_downstream(downstream)
            Attaches the given PipeElement as a downstream flow destination

        to_config()
            Creates serializable PipeElement

        """
        PipeElement.__init__(self, **kwargs)
        if isinstance(columns, list) or columns is None:
            self.columns = columns
        else:
            raise ValueError(
                "Given columns has to be of type list or None"
            )

        self.sparse = sparse
        self.absent_ignore = absent_ignore
        self.keep = keep

    def transform(self, x):
        """Returns categorical data based on the columns

        Arguments
        ------------------
        x : pandas.DataFrame
            The data that should be transformed into categorical data

        Returns
        ------------------
        x : pandas.DataFrame
            Transformed data

        """
        found_columns = []
        if self.columns is None:
            self.columns = list(x.columns)
        for col in self.columns:
            try:
                x[col] = x[col].astype('category')
                found_columns.append(col)
            except KeyError:
                if self.absent_ignore:
                    pass
                else:
                    raise KeyError(f"could not find {col} in DataFrame")
        if self.sparse:
            if self.keep:
                return pd.concat(x, pd.get_dummies(x[found_columns]), axis=1)
            else:
                return pd.get_dummies(x, columns=found_columns)
        else:
            x[found_columns] = x[found_columns].apply(lambda i: i.cat.codes)
            return x
