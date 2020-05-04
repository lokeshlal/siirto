import sys
import os
from siirto.exceptions import SiirtoException
from typing import Callable, Any
from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.shared.enums import PlugInType


class PgDefaultFullLoadPlugin(FullLoadBase):
    """
    Postgres full load default plugin.

    :param output_folder_location: location to write the Copy command data
    :type output_folder_location: str
    :param connection: postgres connection object
    :type connection: psycopg2 connection object
    :param table_name: table name to be copied
    :type table_name: str
    """

    # plugin type and plugin name
    plugin_type = PlugInType.Full_Load
    plugin_name = "PGDefaultFullLoad"

    def __init(self,
               output_folder_location: str = None,
               connection: Any = None,
               table_name: str = None,
               *args,
               **kwargs) -> None:
        if output_folder_location is None:
            raise ValueError("output_folder_location is empty")
        if connection is None:
            raise ValueError("Connection is None")
        if table_name is None:
            raise ValueError("Table name is None")
        super().__init__(*args, **kwargs)
        self.output_folder_location = output_folder_location
        self.status = "not started"
        self.cursor = None
        self.connection = connection
        self.table_name = table_name

    def _set_status(self, status):
        self.status = status

    def execute(self):
        self._set_status("in progress - started")
        cursor = self.connection.cursor()
        copy_query = f"\\COPY {self.table_name} TO program 'split -dl 1000000 " \
                     f"--a _{self.table_name}.csv' (format csv)"
        file_to_write = os.path.join(self.output_folder_location, f"{self.table_name}_full.csv")
        with open(file_to_write, "w") as output_file:
            cursor.copy_to(output_file, self.table_name)
        self._set_status("in progress - bulk file created")
        split_command = f'split -dl 1000000 {self.table_name}_full.csv --a _{self.table_name}.csv'
        os.system(split_command)
        self._set_status("in progress - smaller files created")
        self._set_status("completed")
        if self.notify_on_completion is not None:
            self.notify_on_completion({'status': 'success'})
