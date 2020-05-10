import importlib
import os
from typing import List

from siirto.base import Base
from siirto.shared.enums import PlugInType


class CDCBase(Base):
    """
    CDC base plugin.
    All CDC plugins will be derived from this plugin
    To derive this class, you are expected to override the constructor
    as well as the 'execute' method.

    This run a single instance for all the tables required to
    collect CDC.

    :param output_folder_location: location to write the Copy
        command data
    :type output_folder_location: str
    :param connection_string: postgres connection string
    :type connection_string: str
    :param table_names: table names to be copied. Table name should
        be schema.table_name. for example, public.user_detail
    :type table_names: List[str]
       """

    # plugin type and plugin name
    plugin_type = PlugInType.CDC
    plugin_name = None

    def __init__(self,
                 output_folder_location: str = None,
                 connection_string: str = None,
                 table_names: List[str] = [],
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if output_folder_location is None \
                or len(output_folder_location.strip()) == 0:
            ex_msg = "output_folder_location is empty"
            raise ValueError(ex_msg)
        if connection_string is None \
                or len(connection_string.strip()) == 0:
            ex_msg = "Connection string is None"
            raise ValueError(ex_msg)
        if table_names is None \
                or not isinstance(table_names, list):
            ex_msg = "Table name is None or Empty"
            raise ValueError(ex_msg)
        self.output_folder_location = output_folder_location
        self.connection_string = connection_string
        self.table_names = table_names
        self.is_running = True
        self.status = "not started"

    def execute(self):
        """
        This is the main method to derive when implementing an operator.
        """
        raise NotImplementedError()

    def setup_graceful_shutdown(self):
        """
        Handles graceful shutdown in case of failures
        """
        raise NotImplementedError()

    @classmethod
    def get_object(cls, plugin_name: str):
        """
        Factory.
        Get the cdc plugin object having name `plugin_name`
        :param plugin_name: plugin name
        :return: cdc plug_in
        """
        return next((sub_class for sub_class in CDCBase.__subclasses__()
                     if sub_class.plugin_type == PlugInType.CDC
                     and sub_class.plugin_name == plugin_name), None)

    @classmethod
    def load_derived_classes(cls):
        """
        load the derived cdc plugin classes available in the current directory and
        ending with _plugin.py
        """
        current_directory_path = os.path.dirname(__file__)
        plugin_files = [file_path.replace(".py", "") for file_path in os.listdir(current_directory_path)
                        if file_path.endswith("_plugin.py")]
        for plugin_file in plugin_files:
            importlib.import_module(f"siirto.plugins.cdc.{plugin_file}")
