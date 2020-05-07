import os
import time
import threading
import psycopg2
import re
import subprocess
from typing import List

from siirto.shared.enums import LoadType, DatabaseOperatorType
from tests.test_base.base_test import BaseTest
from siirto.database_operators.postgres_operator import PostgresOperator


class TestPostgresOperator(BaseTest):

    def test_postgres_operator_operator_type_and_name(self):
        self.assertEqual(PostgresOperator.operator_type,
                         DatabaseOperatorType.Postgres)
        self.assertEqual(PostgresOperator.operator_name,
                         "Postgres-Default")

    def test_postgres_operator_operator_init(self):
        database_operator_params = {
            "connection_string": "connection_string",
            "load_type": LoadType.Full_Load_And_CDC,
            "table_names": ['public.employees'],
            "full_load_plugin_name": "PgDefaultFullLoadPlugin",
            "cdc_plugin_name": "PgDefaultCDCPlugin",
            "output_location": "/mnt/d/delete_me",
        }
        b = PostgresOperator(**database_operator_params)

    def test_postgres_operator_operator_init_fail_full_load(self):
        with self.assertRaises(ValueError) as cm:
            database_operator_params = {
                "connection_string": "connection_string",
                "load_type": LoadType.Full_Load_And_CDC,
                "table_names": ['public.employees'],
                "full_load_plugin_name": "PgDefaultFullLoadPlugin_InCorrect",
                "cdc_plugin_name": "PgDefaultCDCPlugin",
                "output_location": "/mnt/d/delete_me",
            }
            b = PostgresOperator(**database_operator_params)
        self.assertEqual(str(cm.exception), "Incorrect value provided for "
                                            "full load plugin `PgDefaultFullLoadPlugin_InCorrect`")

    def test_postgres_operator_operator_init_fail_cdc(self):
        # build the connection string
        with self.assertRaises(ValueError) as cm:
            database_operator_params = {
                "connection_string": self.postgres_connection_string,
                "load_type": LoadType.Full_Load_And_CDC,
                "table_names": ['public.employee'],
                "full_load_plugin_name": "PgDefaultFullLoadPlugin",
                "cdc_plugin_name": "PgDefaultCDCPlugin_InCorrect",
                "output_location": "/mnt/d/delete_me",
            }
            b = PostgresOperator(**database_operator_params)
        self.assertEqual(str(cm.exception), "Incorrect value provided for "
                                            "cdc plugin `PgDefaultCDCPlugin_InCorrect`")

    def insert_ref_data(self):
        with psycopg2.connect(self.postgres_connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('DELETE FROM employee;')
                cursor.execute("INSERT INTO employee VALUES (1, 'User1')")
                cursor.execute("INSERT INTO employee VALUES (2, 'User2')")

    def test_postgres_operator_operator_run_test(self):
        # build the connection string
        self.insert_ref_data()
        output_folder = os.environ["output_folder_path"]
        database_operator_params = {
            "connection_string": self.postgres_connection_string,
            "load_type": LoadType.Full_Load_And_CDC,
            "table_names": ['public.employee'],
            "full_load_plugin_name": "PgDefaultFullLoadPlugin",
            "cdc_plugin_name": "PgDefaultCDCPlugin",
            "output_location": output_folder,
        }
        b = PostgresOperator(**database_operator_params)

        # start a thread to terminate the process
        def thread_to_terminate_processes(postgres_operator_object: PostgresOperator):
            time.sleep(5)
            postgres_operator_object.terminate_process_signal = True

        def insert_ref_cdc_data(connection_string: str):
            time.sleep(1)
            with psycopg2.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO employee VALUES (3, 'User3')")

        threading.Thread(target=thread_to_terminate_processes,
                         args=(b,)).start()
        threading.Thread(target=insert_ref_cdc_data,
                         args=(self.postgres_connection_string,)).start()
        b.execute()
        full_load_output_path = os.path.join(output_folder,
                                             "full_load",
                                             "public_employee",
                                             "x00_public.employee.csv")
        cdc_output_path = os.path.join(output_folder,
                                       "cdc",
                                       "public_employee",
                                       "public.employee_cdc_1.csv")
        with open(full_load_output_path, 'r') as full_load_file:
            full_load_output_content = full_load_file.read()
        with open(cdc_output_path, 'r') as cdc_file:
            cdc_output_content = cdc_file.read()
        self.assertEqual(full_load_output_content, "1\tUser1\n2\tUser2\n")
        self.assertEqual(cdc_output_content,
                         '{"kind": "insert", "schema": "public", "table": "employee", '
                         '"columnnames": ["id", "name"], "columntypes": ["integer", '
                         '"character varying(50)"], "columnvalues": [3, "User3"]}')

    def test_postgres_operator_operator_run_test_full_load_only(self):
        # build the connection string
        self.insert_ref_data()
        output_folder = os.environ["output_folder_path"]
        database_operator_params = {
            "connection_string": self.postgres_connection_string,
            "load_type": LoadType.Full_Load,
            "table_names": ['public.employee'],
            "full_load_plugin_name": "PgDefaultFullLoadPlugin",
            "cdc_plugin_name": None,
            "output_location": output_folder,
        }
        b = PostgresOperator(**database_operator_params)

        # start a thread to terminate the process
        def thread_to_terminate_processes(postgres_operator_object: PostgresOperator):
            time.sleep(5)
            postgres_operator_object.terminate_process_signal = True

        def insert_ref_cdc_data(connection_string: str):
            time.sleep(1)
            with psycopg2.connect(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO employee VALUES (3, 'User3')")

        threading.Thread(target=thread_to_terminate_processes,
                         args=(b,)).start()
        threading.Thread(target=insert_ref_cdc_data,
                         args=(self.postgres_connection_string,)).start()
        b.execute()
        full_load_output_path = os.path.join(output_folder,
                                             "full_load",
                                             "public_employee",
                                             "x00_public.employee.csv")
        cdc_output_path = os.path.join(output_folder, "cdc")
        with open(full_load_output_path, 'r') as full_load_file:
            full_load_output_content = full_load_file.read()
        self.assertEqual(full_load_output_content, "1\tUser1\n2\tUser2\n")
        if os.path.exists(cdc_output_path):
            self.assertFalse("CDC output created for Full Load only job")

    def test_postgres_operator_operator_run_test_cdc_only(self):
        # build the connection string
        self.insert_ref_data()
        output_folder = os.environ["output_folder_path"]
        database_operator_params = {
            "connection_string": self.postgres_connection_string,
            "load_type": LoadType.CDC,
            "table_names": ['public.employee'],
            "full_load_plugin_name": None,
            "cdc_plugin_name": "PgDefaultCDCPlugin",
            "output_location": output_folder,
        }
        b = PostgresOperator(**database_operator_params)

        # start a thread to terminate the process
        def thread_to_terminate_processes(postgres_operator_object: PostgresOperator):
            time.sleep(5)
            postgres_operator_object.terminate_process_signal = True

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
                         args=(b,)).start()
        threading.Thread(target=insert_ref_cdc_data,
                         args=(self.postgres_connection_string,)).start()
        b.execute()
        full_load_output_path = os.path.join(output_folder,
                                             "full_load")

        cdc_output_folder = os.path.join(output_folder,
                                         "cdc",
                                         "public_employee")
        file_indexes = [int(file_name.replace(f"public.employee_cdc_", "").replace(".csv", ""))
                        for file_name in list(os.listdir(cdc_output_folder))
                        if re.search("^public.employee_cdc_.*.csv$", file_name)]
        file_index = max(file_indexes)
        cdc_output_path_1 = os.path.join(output_folder,
                                         "cdc",
                                         "public_employee",
                                         f"public.employee_cdc_{file_index - 1}.csv")
        cdc_output_path_2 = os.path.join(output_folder,
                                         "cdc",
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
        if os.path.exists(full_load_output_path):
            self.assertFalse("Full Load output created for CDC only job")

    def test_process_count(self):
        # build the connection string
        self.insert_ref_data()
        result_queue = []
        output_folder = os.environ["output_folder_path"]
        database_operator_params = {
            "connection_string": self.postgres_connection_string,
            "load_type": LoadType.Full_Load_And_CDC,
            "table_names": ['public.employee'],
            "full_load_plugin_name": "PgDefaultFullLoadPlugin",
            "cdc_plugin_name": "PgDefaultCDCPlugin",
            "output_location": output_folder,
        }
        b = PostgresOperator(**database_operator_params)

        # start a thread to terminate the process
        def thread_to_terminate_processes(postgres_operator_object: PostgresOperator):
            time.sleep(5)
            postgres_operator_object.terminate_process_signal = True

        threading.Thread(target=thread_to_terminate_processes,
                         args=(b,)).start()

        num_of_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
        result_queue.append(num_of_process)

        def get_process_count(queue: List):
            time.sleep(1)
            _num_of_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
            queue.append(_num_of_process)

        before_start_thread = threading.Thread(target=get_process_count,
                                               args=(result_queue, ))
        before_start_thread.start()
        b.execute()
        num_of_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
        result_queue.append(num_of_process)
        self.assertEqual(len(result_queue), 3)
        self.assertEqual(result_queue[0], result_queue[2])
        self.assertEqual(result_queue[1], result_queue[2] + 3)
