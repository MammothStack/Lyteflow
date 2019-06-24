"""Base class for PipeSystem and PipeElement

"""

# Standard library imports


# Third party imports


# Local application imports


class Base:
    def __init__(self, **kwargs):
        """Abstract base class

        Base class for setting up uniform parameters. These parameters are used
        for all pipe connecting elements as well as the whole PipeSystem.

        Properties
        ------------------
        name : str (default: class name)
            The name of the class

        input_dimensions : tuple
            The dimension of the input

        output_dimensions : tuple
            The dimensino of the output

        input_columns : list
            The column names of the input if the input is a pandas DataFrame

        output_columns : list
            The column names of the output if the output is a pandas DataFrame

        """

        self.input_dimensions = None
        self.output_dimensions = None
        self.input_columns = None
        self.output_columns = None
        self.name = kwargs.get("name")
        if not self.name:
            self.name = self.__class__.__name__

    def flow(self, x):
        raise NotImplementedError





