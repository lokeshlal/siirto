import os
import time
import psycopg2
import re
import threading

from siirto.plugins.cdc.pg_default_cdc_plugin import PgDefaultCDCPlugin
from siirto.shared.enums import PlugInType
from tests.test_base.base_test import BaseTest


class TestPgDefaultCDCPlugin(BaseTest):

    def test_postgres_default_cdc_plugin_type_and_name(self):
        self.assertEqual(PgDefaultCDCPlugin.plugin_type,
                         PlugInType.CDC)
        self.assertEqual(PgDefaultCDCPlugin.plugin_name,
                         "PgDefaultCDCPlugin")

    def test_postgres_default_cdc_plugin_init(self):
        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_names": ['public.employee'],
        }
        PgDefaultCDCPlugin(**cdc_init_params)

    def insert_ref_data(self):
        with psycopg2.connect(self.postgres_connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM employee;')
                cursor.execute("INSERT INTO employee VALUES (1, 'User1')")
                cursor.execute("INSERT INTO employee VALUES (2, 'User2')")

    def test_postgres_default_cdc_plugin_run_test(self):
        # build the connection string
        self.insert_ref_data()
        output_folder = os.environ["output_folder_path"]

        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_names": ['public.employee'],
        }
        cdc_plugin_object = PgDefaultCDCPlugin(**cdc_init_params)

        def thread_to_terminate_processes(cdc_plugin_object_: PgDefaultCDCPlugin):
            time.sleep(5)
            cdc_plugin_object_.is_running = False

        def insert_ref_cdc_data(connection_string: str):
            time.sleep(1)
            with psycopg2.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO employee VALUES (3, 'User3')")
                    cursor.execute("INSERT INTO employee VALUES (4, 'User4')")
            time.sleep(2)
            with psycopg2.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO employee VALUES (5, 'User5')")

        threading.Thread(target=thread_to_terminate_processes,
                         args=(cdc_plugin_object, )).start()
        threading.Thread(target=insert_ref_cdc_data,
                         args=(self.postgres_connection_string, )).start()
        cdc_plugin_object.execute()

        cdc_output_folder = os.path.join(self.output_folder,
                                         "public_employee")
        file_indexes = [int(file_name.replace(f"public.employee_cdc_", "").replace(".csv", ""))
                        for file_name in list(os.listdir(cdc_output_folder))
                        if re.search("^public.employee_cdc_.*.csv$", file_name)]
        file_index = max(file_indexes)
        cdc_output_path_1 = os.path.join(self.output_folder,
                                         "public_employee",
                                         f"public.employee_cdc_{file_index - 1}.csv")
        cdc_output_path_2 = os.path.join(self.output_folder,
                                         "public_employee",
                                         f"public.employee_cdc_{file_index}.csv")

        with open(cdc_output_path_1, 'r') as cdc_file:
            cdc_output_content_1 = cdc_file.read()
        with open(cdc_output_path_2, 'r') as cdc_file:
            cdc_output_content_2 = cdc_file.read()
        self.assertEqual(cdc_output_content_1,
                         '{"kind": "insert", "schema": "public", "table": "employee", '
                         '"columnnames": ["id", "name"], "columntypes": ["integer", '
                         '"character varying(50)"], "columnvalues": [3, "User3"]}\n'
                         '{"kind": "insert", "schema": "public", "table": "employee", '
                         '"columnnames": ["id", "name"], "columntypes": ["integer", '
                         '"character varying(50)"], "columnvalues": [4, "User4"]}'
                         )
        self.assertEqual(cdc_output_content_2,
                         '{"kind": "insert", "schema": "public", "table": "employee", '
                         '"columnnames": ["id", "name"], "columntypes": ["integer", '
                         '"character varying(50)"], "columnvalues": [5, "User5"]}')
