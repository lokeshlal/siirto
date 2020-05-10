import importlib
import os
from typing import Callable

from siirto.base import Base
from siirto.shared.enums import PlugInType


class FullLoadBase(Base):
    """
    Full load base plugin.
    All full load plugins will be derived from this plugin
    To derive this class, you are expected to override the constructor as well
    as the 'execute' method.

    This runs a single instance for a single table.

    :param output_folder_location: location to write the Copy command data
    :type output_folder_location: str
    :param connection_string: postgres connection string
    :type connection: str
    :param table_name: table name to be copied
    :type table_name: str
    :param notify_on_completion: callback on completion of full load process
    :type notify_on_completion: Callable[[Any], None]
    """

    # plugin type and plugin name
    plugin_type = PlugInType.Full_Load
    plugin_name = None

    def __init__(self,
                 output_folder_location: str = None,
                 connection_string: str = None,
                 table_name: str = None,
                 notify_on_completion: Callable[[str, str, str], None] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.notify_on_completion = notify_on_completion
        if output_folder_location is None \
                or len(output_folder_location.strip()) == 0:
            ex_msg = "output_folder_location is empty"
            raise ValueError(ex_msg)
        if connection_string is None \
                or len(connection_string.strip()) == 0:
            ex_msg = "Connection string is None"
            raise ValueError(ex_msg)
        if table_name is None \
                or len(table_name.strip()) == 0:
            print(table_name)
            ex_msg = "Table name is None"
            raise ValueError(ex_msg)
        self.output_folder_location = output_folder_location
        self.status = "not started"
        self.connection_string = connection_string
        self.table_name = table_name

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
        Get the full load plugin object having name `plugin_name`
        :param plugin_name: plugin name
        :return: full load plug_in
        """
        return next((sub_class for sub_class in FullLoadBase.__subclasses__()
                     if sub_class.plugin_type == PlugInType.Full_Load
                     and sub_class.plugin_name == plugin_name), None)

    @classmethod
    def load_derived_classes(cls):
        """
        load the derived full load plugin classes available in the current directory and
        ending with _plugin.py
        """
        current_directory_path = os.path.dirname(__file__)
        plugin_files = [file_path.replace(".py", "") for file_path in os.listdir(current_directory_path)
                        if file_path.endswith("_plugin.py")]
        for plugin_file in plugin_files:
            importlib.import_module(f"siirto.plugins.full_load.{plugin_file}")
