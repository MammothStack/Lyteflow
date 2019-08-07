"""Module for splitting an input into multiple outputs

This module contains all PipeElements that handle a single input, but multiple outputs.
This occurs because the input is split along some axis into 2 or more objects or the same
object is duplicated.

"""

# Standard library imports
import warnings

# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement, FlowData
from lyteflow.util import column_names_to_formatted_list


class _Split(PipeElement):
    """Abstract class for all PipeElements that have multiple outputs

    This class is a subclass of PipeElement and inherits and preserves most of its
    functions. It only differs in that it allows multiple downstream elements to be
    attached to this PipeElement. Due to multiple outputs being allowed the flow method
    also needs to be adjusted to accommodate the multiple outputs produced from the
    transform function.

    There is an inherent conflict between the argument n_result in the constructor and
    the amount of downstream elements given. n_result sets the expected results that
    should be produced from the transform function. However n_result cannot be set
    with multiple downstream elements as the amount of downstream elements is also used
    to produce the amount of downstream elements set. If the n_result is set and
    multiple downstream elements have been set then n_result will be changed to the
    amount of downstream elements.

    All other properties and methods have been kept from the super class PipeElement

    Methods
    ------------------
    flow(x)
        Receives FlowData from upstream, transforms, produces FlowData for downstream

    attach_downstream(*downstream)
        Attaches downstream PipeElements

    """

    def __init__(self, n_result=None, **kwargs):
        """Constructor for _Split

        Arguments
        ------------------
        n_result : int
            The amount of FlowData that should be produced. Will be changed to amount
            of downstream elements if more than one downstream element is set

        """
        if n_result is not None and n_result < 1:
            raise ValueError("Expected number of outputs cannot be less than 1")
        self.n_result = n_result
        PipeElement.__init__(self, **kwargs)

    def flow(self, x):
        """Receives FlowData from upstream, transforms, produces FlowData for downstream

        Before data transformations can occur, it will check that its upstream
        PipeElements have all executed, as well as it's requirement's PipeElements. In
        case of the Requirements this is essential for correct execution. It will check
        that the given FlowData element has the correct "to_element" to ensure the data
        ended up where it was supposed to.

        The Requirements are iterated through and the defined class variables are
        changed to the values set in the defined PipeElements. Input and output shape
        and dimensions are set, as well as a class variable indicating that the
        PipeElement has executed. The FlowData is produced and returned as a list of
        the length of the downstream PipeElements or the length set by the n_output
        variable

        Arguments
        ------------------
        x : FlowData
            The input flow that should be transformed and passed downstream

        Returns
        ------------------
        *x : FlowData
            A tuple of FlowData which is a the output of the data transformation that
            should be passed downstream

        Raises
        ------------------
        AttributeError
            When not all preset PipeElements and Requirements have executed

        ValueError
            When the given FlowData is not addressed to this PipeElement

        AttributeError
            When the produced data does not match the expected outcome

        """
        self._flow_preset_check(x)
        transformed_x = self.transform(x.data)
        self._flow_postset_check(*transformed_x)
        self._executed = True
        self._n_output = self.n_result

        if len(self.downstream) > 1:
            matched = zip([y for y in transformed_x], [d for d in self.downstream])
            return tuple(
                FlowData(from_element=self, data=z[0], to_element=z[1]) for z in matched
            )
        else:
            return tuple(
                FlowData(from_element=self, data=y, to_element=self.downstream[0])
                for y in transformed_x
            )

    def attach_downstream(self, *downstream):
        """Attaches downstream PipeElements

        Arguments
        ------------------
        *downstream : PipeElement
            The PipeElements that should be added to downstream


        """
        self.downstream += downstream

    def _flow_preset_check(self, x):
        """Checks the preset configuration and data

        Calls super and adds checks for the proper amount of outputs

        Arguments
        ------------------
        x : FlowData
            The data to be checked and set

        Raises
        ------------------
        AttributeError
            When not all preset PipeElements and Requirements have executed

        ValueError
            When the given FlowData is not addressed to this PipeElement

        """

        super()._flow_preset_check(x)
        if self.n_result is None:
            self.n_result = len(self.downstream)
        else:
            if self.n_result != len(self.downstream) and len(self.downstream) != 1:
                pre = self.n_result
                self.n_result = len(self.downstream)
                warnings.warn(
                    f"n_result changed from {pre} to {self.n_result} as multiple "
                    f"downstream elements have been set. n_result can only differ from "
                    f"the amount of downstream elements when there is only element",
                    UserWarning,
                )

    def _flow_postset_check(self, *x):
        """Checks the postset configuration and data

        Arguments
        ------------------
        *x : numpy.array/pandas.DataFrame
            Tuple of data from transform method

        Raises
        ------------------
        AttributeError
            When the produced data does not match the expected outcome

        """

        if len(x) != self.n_result:
            raise AttributeError(
                f"Amount of data produced does not equal expected "
                f"result. produced: {len(x)} expected: {self.n_result}"
            )

        try:
            self.output_dimensions = [y.shape for y in x]
            self.output_columns = [y.columns for y in x]
        except AttributeError:
            pass


class Duplicator(_Split):
    """Duplicates the input

    Overrides the transform function in order to duplicate all the given inputs

    Examples
    ------------------
    >>>from lyteflow.kernels.split import Duplicator
    >>>import numpy as np
    >>>dup = Duplicator(n_result=3)
    >>>data = np.array([[1,2,3]])
    >>>dup.transform(data)
    [array([[1, 2, 3]]), array([[1, 2, 3]]), array([[1, 2, 3]])]

    """

    def transform(self, x):
        """Duplicates the given input n. n = n_result or length of downstream elements

        Arguments
        ------------------
        x : pd.DataFrame, np.array
            DataFrame that should be duplicated

        Returns
        ------------------
        output : list
            List of DataFrames/array with length n where n is the amount of sets of
            columns

        """

        return [x.copy() for i in range(self.n_result)]


class ColumnSplitter(_Split):
    """Splits the input along the given columns

    The method of splitting and therefore resulting data depends on the format of the
    "columns" argument in the constructor. Passing a string (column name) will result in
    only that column being split from the rest. A list of strings (column names) will
    result in those columns being split together. A list of lists will result in each
    list being split individually. See examples.

    Examples
    ------------------
    Splitting a DataFrame into two outputs by selecting columns ["a","b","c"] and
    another DataFrame containing the rest of the columns:

    >>>import pandas as pd
    >>>import numpy as np
    >>>from lyteflow.kernels.split import ColumnSplitter
    >>>df = pd.DataFrame(np.random.randint(0,2,(3,5)), columns=["a","b","c","d","e"])
    >>>cs = ColumnSplitter(columns=["a","b","c"], split_rest=True)
    >>>cs.transform(df)
    [   a  b  c
    0  0  1  1
    1  0  0  0
    2  1  0  0,    d  e
    0  1  0
    1  1  0
    2  0  0]

    """

    def __init__(self, columns, split_rest=False, **kwargs):
        """Constructor for ColumnSplitter

        Arguments
        ------------------
        columns : list
            A list of column names that should be split from the given data.

        split_rest : bool
            If all columns not listed in the "columns" argument should be given as an
            output

        """
        self.columns = column_names_to_formatted_list(columns)
        self.split_rest = split_rest
        kwargs["n_result"] = None
        _Split.__init__(self, **kwargs)

    def transform(self, x):
        """Splits the DataFrame along the columns set in the constructor

        Arguments
        ------------------
        x : pd.DataFrame
            DataFrame that should be split

        Returns
        ------------------
        output : list
            List of DataFrames with length n where n is the amount of sets of columns

        """
        if self.split_rest:
            all_columns = [a for b in self.columns for a in b]
            final_columns = self.columns + [list(x.columns.difference(all_columns))]
        else:
            final_columns = self.columns

        self._n_output = len(final_columns)
        return [x.loc[:, x.columns.intersection(col)] for col in final_columns]
