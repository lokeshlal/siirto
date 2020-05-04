import sys
import os
import glob
from siirto.exceptions import SiirtoException
from typing import List, Any
from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.shared.enums import PlugInType


class PgDefaultCDCPlugin(FullLoadBase):
    """
    Postgres CDC default plugin.

    :param output_folder_location: location to write the Copy
        command data
    :type output_folder_location: str
    :param connection: postgres connection object
    :type connection: psycopg2 connection object
    :param table_names: table names to be copied
    :type table_names: List[str]
    :param buffer_size: buffer size (no of lines) before creating
        a new cdc file
    :type buffer_szie: int
    """

    # plugin type and plugin name
    plugin_type = PlugInType.Full_Load
    plugin_name = "PGDefaultFullLoad"

    def __init(self,
               output_folder_location: str = None,
               connection: Any = None,
               table_names: List[str] = None,
               buffer_size: int = 1000,
               *args,
               **kwargs) -> None:
        if output_folder_location is None:
            raise ValueError("output_folder_location is empty")
        if connection is None:
            raise ValueError("Connection is None")
        if table_names is None \
                or not isinstance(table_names, type(list)) \
                or len(table_names) == 0:
            raise ValueError("Table name is None or Empty")
        super().__init__(*args, **kwargs)
        self.output_folder_location = output_folder_location
        self.status = "not started"
        self.cursor = None
        self.connection = connection
        self.table_names = table_names

    def _set_status(self, status):
        self.status = status

    def execute(self):
        self._set_status("in progress - started")
        cursor = self.connection.cursor()
        slot_name = "siirto_slot"
        cursor.execute(f"SELECT 'init' FROM "
                       f"pg_create_logical_replication_slot('{slot_name}', 'wal2json');")

        current_table_cdc_file_name = {}
        for table_name in self.table_names:
            file_indexes = [int(index.replace(f"{table_name}_cdc_", "").replace(".csv", ""))
                            for index in glob.glob(f"{table_name}_cdc_*.csv")]
            if len(file_indexes) == 0:
                file_index = 0
            else:
                file_index = max(file_indexes) + 1

            file_to_write = os.path.join(self.output_folder_location,
                                         f"{table_name}_cdc_{file_index}.csv")
            current_table_cdc_file_name[table_name] = file_to_write

        tables_string = ",".join(self.table_names)
        while True:
            rows = cursor.execute(f"SELECT lsn, data FROM  pg_logical_slot_peek_changes('{slot_name}', "
                                  f"NULL, NULL, 'pretty-print', '1', "
                                  f"'add-tables', '{tables_string}');")
            rows_collected = {}

            for row in rows:
                pass

            if len(rows_collected.keys()) > 0:
                # write the data to files
                for table_name in rows_collected.keys():
                    # current_table_cdc_file_name
                    pass

            with open(file_to_write, "a+") as output_file:
                output_file.write(cdc_data)
            self._set_status("in progress - bulk file created")
            split_command = f'split -dl 1000000 {self.table_name}_full.csv --a _{self.table_name}.csv'
            os.system(split_command)
            self._set_status("in progress - smaller files created")
            self._set_status("completed")
            if self.notify_on_completion is not None:
                self.notify_on_completion({'status': 'success'})
