import glob
import json
import os
import time
import re

import psycopg2

from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.shared.enums import PlugInType


class PgDefaultCDCPlugin(CDCBase):
    """
    Postgres CDC default plugin.

    :param poll_frequency: frequency at which Postresql database
        should be polled. Default is `1` seconds.
    :type poll_frequency: int
    """

    # plugin type and plugin name
    plugin_type = PlugInType.CDC
    plugin_name = "PgDefaultCDCPlugin"
    plugin_parameters = {
        "poll_frequency": {
            "type": int
        }
    }

    def __init__(self,
                 poll_frequency: int = 1,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.poll_frequency = poll_frequency

    def _set_status(self, status: str):
        """
        Set the status of the running plugin
        :param status: status
        """
        status = f"CDC - {status}"
        self.status = status
        self.logger.info(status)

    def execute(self):
        self.logger.info("in progress - started")
        connection = psycopg2.connect(self.connection_string)
        cursor = connection.cursor()
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

        current_table_cdc_file_name = {}
        for table_name in self.table_names:
            table_name_in_folder = table_name.replace(".", "_")
            cdc_folder_for_table = os.path.join(self.output_folder_location,
                                                table_name_in_folder)
            file_indexes = []
            if os.path.exists(cdc_folder_for_table):
                file_indexes = [int(file_name.replace(f"{table_name}_cdc_", "").replace(".csv", ""))
                                for file_name in list(os.listdir(cdc_folder_for_table))
                                if re.search(f"^{table_name}_cdc_.*.csv$", file_name)]

            file_index = 1
            if len(file_indexes) > 0:
                file_index = max(file_indexes) + 1

            if not os.path.exists(cdc_folder_for_table):
                os.mkdir(cdc_folder_for_table)

            file_to_write = os.path.join(self.output_folder_location,
                                         table_name_in_folder,
                                         f"{table_name}_cdc_{file_index}.csv")
            current_table_cdc_file_name[table_name] = {
                'file_to_write': file_to_write,
                'index': file_index
            }

        tables_string = ",".join(self.table_names)
        while self.is_running:
            self.logger.info("running cdc pull iteration")
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
                        else:
                            rows_collected[table_name] = [json.dumps(change_set_entry)]

            # persist the WALs
            if len(rows_collected.keys()) > 0:
                cdc_captured_details = {}
                # write the data to files
                for table_name in rows_collected.keys():
                    name_and_index = current_table_cdc_file_name[table_name]
                    cdc_captured_details[table_name] = len(rows_collected[table_name])
                    with open(name_and_index["file_to_write"], "w+") as output_file:
                        output_file.write("\n".join(rows_collected[table_name]))
                        new_file_index = name_and_index['index'] + 1
                        new_file_to_write = os.path.join(self.output_folder_location,
                                                         table_name.replace(".", "_"),
                                                         f"{table_name}_cdc_{new_file_index}.csv")
                        current_table_cdc_file_name[table_name] = {
                            'file_to_write': new_file_to_write,
                            'index': new_file_index
                        }

                self.logger.info(f"Following tables has change data: {cdc_captured_details}")

            # remove the WALs
            if max_lsn:
                cursor.execute(f"SELECT 1 FROM  pg_logical_slot_get_changes('{slot_name}', "
                               f"'{max_lsn}', NULL, 'pretty-print', '1', "
                               f"'add-tables', '{tables_string}');")
            # sleep for one second, before next pool
            time.sleep(self.poll_frequency)
        self.logger.info("stopped")
