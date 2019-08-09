"""Module for PipeElements that process strings

This module contains PipeElements that analyze the contents, and transform
parts of the data based on the contents of the strings. This can be useful
for cleaning data. For example a column that contains categorical data, but
the formatting is wrong will produce bad results when this data is categorised.
For example consider the following column:
    "column a"
1   "a, b"
0   "b, a"

A standard categorisation will categorise these two into categories ("a, b", "b, a"):
    "column a"  "column a_a, b"    "column a_b, a"
0   "a, b"      1                   0    
1   "b, a"      0                   1

However if you were after the values a and b then the following is correct:
    "column a"  "column a_a"    "column a_b"
0   "a, b"      1                   1    
1   "b, a"      1                   1

This transformation can be achieved with the Separator

"""

# Standard library imports
import warnings
import re

# Third party imports
import numpy as np
import pandas as pd

# Local application imports
from lyteflow.kernels.base import PipeElement


def _split_with_delimiter(string, regex_delimiter):
    """Split the given string with regex delimiter
    
    Arguments
    ------------------
    string : str
        The string that should be split
        
    regex_delimiter : str
        The delimiters as regex that should be used to split the string
        
    Returns
    ------------------
    split : list
        List of split strings
        
    """

    return re.split(regex_delimiter, string)


def _strip_string(*string):
    """Strips each element of the given list
    
    Arguments
    ------------------
    *string : str
        string to be stripped
        
    Returns
    ------------------
    stripped : list
        Stripped strings
    
    """
    return [x.strip() for x in string]


def _split_strip_string(string, regex_delimiter):
    """Splits the string and strips its splits
    
    Arguments
    ------------------
    string : str
        The string that should be split
        
    regex_delimiter : str
        The delimiters as regex that should be used to split the string
        
    Returns
    ------------------
    split : list
        List of split strings
    
    """
    return _strip_string(*_split_with_delimiter(string, regex_delimiter))


def _convert_delimiters_to_regex(*delimiters):
    """Converts a list of strings into a regex
    
    Arguments
    ------------------
    *delimiters : str
        The delimiters as that should be converted into a regex
        
    Returns
    ------------------
    regex : str
        The converted string
    
    """

    return "|".join(delimiters)


class Separator(PipeElement):
    """Separate strings in columns and categorise them
    
    This class subclasses from PipeElement and inherits most of its behavior.
    It overrides the function transform, but the rest of PipeElements functions
    are kept.
    
    This class is ideal for extracting categorical string data from columns that
    have comma separate values. The character or string at which the contents are
    seperated can be set by the delimiter argument. The columns argument will
    determine on which columns this extraction is applied. If None are given then
    the transformation is applied to each column of the given DataFrame in
    the transform method.
    
    Once the values have been separated they will be counted into a set of unique
    values, which are iterated over and the contents of each row will be counted
    if the value exists. This creates the extracted categorical data
    
    Attributes
    ------------------
    
    Methods
    ------------------
    transform(x)
        Splits and categorises the data based on the columns and delimiter given
    
    Examples
    ------------------
    Using the Separator to turn one column into categorical data:
    
        >>> from lyteflow.kernels.str import Separator
        >>> import pandas as pd
        >>> df = pd.DataFrame([["apple, pear", "banana, pear"],["pear, banana", "apple, banana"]], columns=["a","b"])
        >>> df
                      a              b
        0   apple, pear   banana, pear
        1  pear, banana  apple, banana
        >>> s = Separator(columns="a", delimiter=",")
        >>> s.transform(df)
                      a              b  a_apple  a_pear  a_banana
        0   apple, pear   banana, pear        1       1         0
        1  pear, banana  apple, banana        0       1         1
    
    Using multiple columns and multiple delimiters to categorise:
        >>> from lyteflow.kernels.str import Separator
        >>> import pandas as pd
        >>> df = pd.DataFrame([["apple; pear / banana", "pear; apple"],["pear,; banana", "apple"]], columns=["a","b"])
        >>> df
                              a            b
        0  apple; pear / banana  pear; apple
        1         pear,; banana        apple
        >>> s = Separator(columns=["a","b"], delimiter=[",", ";", "/"])
        >>> s.transform(df)
                              a            b  a_banana  a_pear  a_apple  b_pear  b_apple
        0  apple; pear / banana  pear; apple         1       1        1       1        1
        1         pear,; banana        apple         1       1        0       0        1
        
        
    """

    def __init__(self, columns=None, delimiter=",", **kwargs):
        """Constructor
        
        Arguments
        ------------------        
        columns : list
            The columns of the DataFrame on which this transformation should be
            applied
            
        delimiter : str or list of str
            The delimiter(s) used to split the string
        
        """
        self.columns = columns
        self.delimiter = delimiter
        PipeElement.__init__(self, **kwargs)

    def transform(self, x):
        """Splits and categorises the data based on the columns and delimiter given
        
        Converts the given delimiter to a regular expression. The given columns are
        iterated through and for each column a series is created where the columns
        contents are split with the delimiter. From this series a unique list of
        values is created by flattening the series. For each unique value a new
        column is created with True or False values depending on if the unique value
        is present in the column or not.
        
        Arguments
        ------------------
        x : pd.DataFrame
            The DataFrame that should be separated and categorised

        Returns
        ------------------
        x : pd.DataFrame
            Transformed DataFrame

        Raises
        ------------------
        KeyError
            When the given column(s) are not present in the DataFrame
        
        
        """

        if self.columns is None:
            self.columns = x.columns
        else:
            if not all([c in x.columns for c in self.columns]):
                raise KeyError("Not all given columns found in DataFrame")

        if isinstance(self.delimiter, str):
            regex_delimiter = _convert_delimiters_to_regex(self.delimiter)
        else:
            regex_delimiter = _convert_delimiters_to_regex(*self.delimiter)

        for column in self.columns:
            series = x[column].map(
                lambda x: _split_strip_string(string=x, regex_delimiter=regex_delimiter)
            )
            unique = set([x for l in series.tolist() for x in l])
            sub_columns = [column + "_" + u for u in unique]
            for u, col in zip(unique, sub_columns):
                x[col] = series.map(lambda y: u in y).astype(int)
        return x
