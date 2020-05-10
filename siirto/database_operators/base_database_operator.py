import importlib
import os
from typing import List

from siirto.shared.enums import LoadType, DatabaseOperatorType
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
    :param table_names: tables which needs to be considered
    :type table_names: List[str]
    :param full_load_plugin_name: name of full load plugin to be used in execute
    :type full_load_plugin_name: str
    :param cdc_plugin_name: name of cdc plugin to be used in execute
    :type cdc_plugin_name: str
    :param output_location: output location for the full load and cdc
    :type output_location: str
    """

    # operator type for the operator
    operator_type = None
    # operator name, which will go in configuration
    operator_name = None

    def __init__(self,
                 connection_string: str,
                 load_type: LoadType = LoadType.Full_Load_And_CDC,
                 table_names: List[str] = [],
                 full_load_plugin_name: str = None,
                 cdc_plugin_name: str = None,
                 output_location: str = None,
                 *args,
                 **kwargs):
        self.connection_string = connection_string
        self.load_type = load_type
        self.full_load_plugin_name = full_load_plugin_name
        self.cdc_plugin_name = cdc_plugin_name
        self.table_names = table_names or []
        self.output_location = output_location
        self.terminate_process_signal = None
        super().__init__(*args, **kwargs)
        self._validate_and_sanitize_input_parameters()

    def _validate_and_sanitize_input_parameters(self) -> None:
        """
        Validates the input parameters.
        Only validate connection string and load type.
        input and output plugins will be validated in corresponding operator
        """
        if len(self.connection_string.strip()) == 0:
            ex_msg = "Empty value provided for connection string"
            raise ValueError(ex_msg)
        if len(self.output_location.strip()) == 0:
            ex_msg = "Empty value provided for output location"
            raise ValueError(ex_msg)
        if (self.load_type is not None and type(self.load_type) is not LoadType) \
                or self.load_type is None:
            ex_msg = f"Incorrect value provided for load type {self.load_type}"
            raise ValueError(ex_msg)
        if not isinstance(self.table_names, list):
            ex_msg = f"Table names `{self.table_names}` should be list or None"
            raise ValueError(ex_msg)
        if len(self.table_names) == 0:
            ex_msg = f"Table names `{self.table_names}` should not be empty"
            raise ValueError(ex_msg)

    def execute(self):
        """
        This is the main method to derive when implementing an operator.
        """
        raise NotImplementedError()

    @classmethod
    def get_object(cls, database_operator_type: str, database_operator_name: str):
        """
        Factory.
        Get the databae operator type object having name `database_operator_name`
        :param database_operator_type: type of operator to load
        :param database_operator_name: name of operator to load
        :return: operator
        """
        return next((sub_class for sub_class in BaseDataBaseOperator.__subclasses__()
                     if sub_class.operator_type == DatabaseOperatorType[database_operator_type]
                     and sub_class.operator_name == database_operator_name), None)

    @classmethod
    def load_derived_classes(cls):
        """
        load the derived operator classes available in the current directory and
        ending with _operator.py
        """
        current_directory_path = os.path.dirname(__file__)
        plugin_files = [file_path.replace(".py", "") for file_path in os.listdir(current_directory_path)
                        if file_path.endswith("_operator.py")]
        for plugin_file in plugin_files:
            importlib.import_module(f"siirto.database_operators.{plugin_file}")
