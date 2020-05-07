import os

import psycopg2

from siirto.plugins.full_load.pg_default_full_load_plugin import PgDefaultFullLoadPlugin
from siirto.shared.enums import PlugInType
from tests.test_base.base_test import BaseTest


class TestPgDefaultFullLoadPlugin(BaseTest):

    def test_postgres_default_full_load_plugin_type_and_name(self):
        self.assertEqual(PgDefaultFullLoadPlugin.plugin_type,
                         PlugInType.Full_Load)
        self.assertEqual(PgDefaultFullLoadPlugin.plugin_name,
                         "PgDefaultFullLoadPlugin")

    def test_postgres_default_full_load_plugin_init(self):
        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": "public.employee",
            "notify_on_completion": None
        }
        PgDefaultFullLoadPlugin(**full_load_init_params)

    def insert_ref_data(self):
        with psycopg2.connect(self.postgres_connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM employee;')
                cursor.execute("INSERT INTO employee VALUES (1, 'User1')")
                cursor.execute("INSERT INTO employee VALUES (2, 'User2')")

    def test_postgres_default_full_load_plugin_run_test(self):
        self.insert_ref_data()
        notification_result_queue = []

        def notification_from_full_load(status: str, table_name: str, error: str) -> None:
            notification_result_queue.append(f"{status}:{table_name}:{error}")

        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": "public.employee",
            "notify_on_completion": notification_from_full_load
        }
        full_load_plugin_object = PgDefaultFullLoadPlugin(**full_load_init_params)

        full_load_plugin_object.execute()
        full_load_output_path = os.path.join(self.output_folder,
                                             "x00_public.employee.csv")
        with open(full_load_output_path, 'r') as full_load_file:
            full_load_output_content = full_load_file.read()
        self.assertEqual(full_load_output_content, "1\tUser1\n2\tUser2\n")
        self.assertEqual(len(notification_result_queue), 1)
        self.assertEqual(notification_result_queue[0], "success:public.employee:None")

    def empty_employee_table(self):
        with psycopg2.connect(self.postgres_connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM employee;')

    def test_postgres_default_full_load_plugin_run_test_empty_table(self):
        self.empty_employee_table()
        notification_result_queue = []

        def notification_from_full_load(status: str, table_name: str, error: str) -> None:
            notification_result_queue.append(f"{status}:{table_name}:{error}")

        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": "public.employee",
            "notify_on_completion": notification_from_full_load
        }
        full_load_plugin_object = PgDefaultFullLoadPlugin(**full_load_init_params)
        full_load_plugin_object.execute()
        full_load_output_path = os.path.join(self.output_folder,
                                             "x00_public.employee.csv")
        with open(full_load_output_path, 'r') as full_load_file:
            full_load_output_content = full_load_file.read()
        self.assertEqual(os.stat(full_load_output_path).st_size, 0)
        self.assertEqual(full_load_output_content, "")
        self.assertEqual(len(notification_result_queue), 1)
        self.assertEqual(notification_result_queue[0], "success:public.employee:Table was empty")

    def insert_ref_data(self):
        with psycopg2.connect(self.postgres_connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM employee;')
                cursor.execute("INSERT INTO employee VALUES (1, 'User1')")
                cursor.execute("INSERT INTO employee VALUES (2, 'User2')")

    def test_postgres_default_full_load_plugin_run_test_split_by_1(self):
        self.insert_ref_data()
        notification_result_queue = []

        def notification_from_full_load(status: str, table_name: str, error: str) -> None:
            notification_result_queue.append(f"{status}:{table_name}:{error}")

        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": "public.employee",
            "notify_on_completion": notification_from_full_load,
            "split_file_size_limit": 1
        }
        full_load_plugin_object = PgDefaultFullLoadPlugin(**full_load_init_params)

        full_load_plugin_object.execute()
        full_load_output_path_0 = os.path.join(self.output_folder,
                                               "x00_public.employee.csv")
        full_load_output_path_1 = os.path.join(self.output_folder,
                                               "x01_public.employee.csv")
        with open(full_load_output_path_0, 'r') as full_load_file:
            full_load_output_content_0 = full_load_file.read()
        with open(full_load_output_path_1, 'r') as full_load_file:
            full_load_output_content_1 = full_load_file.read()
        self.assertEqual(full_load_output_content_0, "1\tUser1\n")
        self.assertEqual(full_load_output_content_1, "2\tUser2\n")
        self.assertEqual(len(notification_result_queue), 1)
        self.assertEqual(notification_result_queue[0], "success:public.employee:None")
