from typing import List, Any
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

    def __init(self,
               output_folder_location: str = None,
               connection_string: str = None,
               table_names: List[str] = [],
               *args,
               **kwargs) -> None:
        if output_folder_location is None:
            raise ValueError("output_folder_location is empty")
        if connection_string is None:
            raise ValueError("Connection string is None")
        if table_names is None \
                or not isinstance(table_names, type(list)):
            raise ValueError("Table name is None or Empty")
        self.output_folder_location = output_folder_location
        self.connection_string = connection_string
        self.table_names = table_names
        self.is_running = True
        self.status = "not started"

        super().__init__(*args, **kwargs)

    def execute(self):
        """
        This is the main method to derive when implementing an operator.
        """
        raise NotImplementedError()
