from siirto.shared.enums import LoadType
from siirto.base import Base


class BaseDataBaseOperator(Base):
    """
    Abstract base class for all database operators.
    To derive this class, you are expected to override the constructor as well
    as the 'execute' method.

    Operators derived from this class should perform following steps:
    1. implement the execute method
        a. implement the full load
        b. implement the CDC
        c. handle the fail-overs
        d. persist the state

    :param connection_string: connection string for the database
    :type connection_string: str
    :param load_type: type of load to run for the job. LoadType.Full_Load,
        LoadType.CDC or LoadType.Full_Load_And_CDC
    :type connection_string: LoadType
    :param input_plugin_name: name of input plugin to be used in execute
    :type input_plugin_name: str
    :param output_plugin_name: name of output plugin to be used in execute
    :type output_plugin_name: str
    """

    # operator type for the operator
    operator_type = None
    # operator name, which will go in configuration
    name = None

    def __init__(self,
                 connection_string: str,
                 load_type: LoadType = LoadType.Full_Load_And_CDC,
                 input_plugin_name: str = None,
                 output_plugin_name: str = None,
                 *args,
                 **kwargs):
        self.connection_string = connection_string
        self.load_type = load_type
        self.input_plugin_name = input_plugin_name
        self.output_plugin_name = output_plugin_name
        super().__init__(*args, **kwargs)
        self._validate_and_sanitize_input_parameters()

    def _validate_and_sanitize_input_parameters(self) -> None:
        """
        Validates the input parameters.
        Only validate connection string and load type.
        input and output plugins will be validated in corresponding operator
        """
        if len(self.connection_string.split()) == 0:
            raise ValueError("Empty value provided for connection string")
        if (self.load_type is not None and type(self.load_type) is not LoadType) \
                or self.load_type is None:
            raise ValueError(f"Incorrect value provided for load type {self.load_type}")

    def execute(self):
        """
        This is the main method to derive when implementing an operator.
        """
        raise NotImplementedError()
