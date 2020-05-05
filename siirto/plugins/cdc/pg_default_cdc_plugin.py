import sys
import os
import glob
import json
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
    :param table_names: table names to be copied. Table name should
        be schema.table_name. for example, public.user_detail
    :type table_names: List[str]
    :param buffer_size: buffer size (no of lines) before creating
        a new cdc file
    :type buffer_size: int
    """

    # plugin type and plugin name
    plugin_type = PlugInType.Full_Load
    plugin_name = "PGDefaultFullLoad"

    def __init(self,
               output_folder_location: str = None,
               connection: Any = None,
               table_names: List[str] = [],
               buffer_size: int = 1000,
               *args,
               **kwargs) -> None:
        if output_folder_location is None:
            raise ValueError("output_folder_location is empty")
        if connection is None:
            raise ValueError("Connection is None")
        if table_names is None \
                or not isinstance(table_names, type(list)):
            raise ValueError("Table name is None or Empty")
        super().__init__(*args, **kwargs)
        self.output_folder_location = output_folder_location
        self.status = "not started"
        self.cursor = None
        self.connection = connection
        # move this to operator class
        self.connection.autocommit = True
        self.table_names = table_names
        self.is_running = True

    def _set_status(self, status):
        self.status = status

    def execute(self):
        self._set_status("in progress - started")
        cursor = self.connection.cursor()
        slot_name = "siirto_slot"

        # create the slot, if doesn't already exists
        cursor.execute(f"SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}';")
        rows = cursor.fetchall()
        cursor_exists = False
        for row in rows:
            cursor_exists = True
        if not cursor_exists:
            cursor.execute(f"SELECT 'init' FROM "
                           f"pg_create_logical_replication_slot('{slot_name}', 'wal2json');")

        # get the tables from postgres
        if len(self.table_names) == 0:
            cursor.execute("\\dt;")
            rows = cursor.fetchall()
            for row in rows:
                self.table_names.append(f"{row[0]}.{row[1]}")

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
            current_table_cdc_file_name[table_name] = {
                'file_to_write': file_to_write,
                'index': file_index
            }

        tables_string = ",".join(self.table_names)
        while self.is_running:
            cursor.execute(f"SELECT lsn, data FROM  pg_logical_slot_peek_changes('{slot_name}', "
                           f"NULL, NULL, 'pretty-print', '1', "
                           f"'add-tables', '{tables_string}');")
            rows = cursor.fetchall()
            rows_collected = {}
            max_lsn = None
            # read the WALs
            for row in rows:
                max_lsn = row[0]
                change_set = json.loads(row[1])
                change_set_entries = change_set["change"] if 'change' in change_set else []
                for change_set_entry in change_set_entries:
                    table_name = f"{change_set_entry['schema']}.{change_set_entry['table']}" \
                        if 'table' in change_set_entry else None
                    if table_name:
                        if table_name in rows_collected:
                            rows_collected[table_name].append(json.dumps(change_set_entry))

            # persist the WALs
            if len(rows_collected.keys()) > 0:
                # write the data to files
                for table_name in rows_collected.keys():
                    name_and_index = current_table_cdc_file_name[table_name]
                    with open(name_and_index["file_to_write"], "w+") as output_file:
                        output_file.write("\n".join(rows_collected[table_name]))
                        new_file_index = name_and_index['index'] + 1
                        new_file_to_write = os.path.join(self.output_folder_location,
                                                         f"{table_name}_cdc_{new_file_index}.csv")
                        current_table_cdc_file_name[table_name] = {
                            'file_to_write': new_file_to_write,
                            'index': new_file_index
                        }

            # remove the WALs
            if max_lsn:
                cursor.execute(f"SELECT 1 FROM  pg_logical_slot_get_changes('{slot_name}', "
                               f"'{max_lsn}', NULL, 'pretty-print', '1', "
                               f"'add-tables', '{tables_string}');")
