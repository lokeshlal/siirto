import psycopg2
import os
from typing import Dict
import multiprocessing

from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.shared.enums import DatabaseOperatorType, PlugInType, LoadType


class PostgresOperator(BaseDataBaseOperator):
    """
    Postgres default operator implements BaseDataBaseOperator
    """

    operator_type = DatabaseOperatorType.Postgres
    operator_name = "Postgres-Default"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.full_load_plugin = next((sub_class for sub_class in CDCBase.__subclasses__()
                                      if sub_class.plugin_type == PlugInType.Full_Load
                                      and sub_class.plugin_name == self.full_load_plugin_name), None)
        self.cdc_plugin = next((sub_class for sub_class in CDCBase.__subclasses__()
                                if sub_class.plugin_type == PlugInType.Full_Load
                                and sub_class.plugin_name == self.cdc_plugin_name), None)

        self._validate_input_parameters()

    def _validate_input_parameters(self):
        if self.full_load_plugin is None:
            if self.load_type in [LoadType.Full_Load, LoadType.Full_Load_And_CDC]:
                raise ValueError(f"Incorrect value "
                                 f"provided for {self.full_load_plugin_name}")
        if self.cdc_plugin is None:
            if self.load_type in [LoadType.CDC, LoadType.Full_Load_And_CDC]:
                raise ValueError(f"Incorrect value "
                                 f"provided for {self.cdc_plugin_name}")

    @staticmethod
    def on_full_load_completed(status: str, table_name: str):
        if status == "success":
            print(f"Full load completed for {table_name}")

    def execute(self):

        full_load_jobs = []

        # start full load
        if self.load_type in [LoadType.Full_Load, LoadType.Full_Load_And_CDC]:
            if len(self.table_names) == 0:
                connection = psycopg2.connect(self.connection_string)
                connection.autocommit = True
                cursor = connection.cursor()
                cursor.execute("\\dt;")
                rows = cursor.fetchall()
                for row in rows:
                    self.table_names.append(f"{row[0]}.{row[1]}")

            for table_name in self.table_names:

                output_location_of_table = os.path.join(self.output_location,
                                                        table_name.replace(".", "_"))
                if not os.path.exists(output_location_of_table):
                    os.mkdir(output_location_of_table)

                full_load_init_params = {
                    "output_folder_location": output_location_of_table,
                    "connection_string": self.connection_string,
                    "table_name": table_name,
                    "notify_on_completion": PostgresOperator.on_full_load_completed
                }

                full_load_object = self.full_load_plugin(**full_load_init_params)
                full_load_process = multiprocessing.Process(
                    target=PostgresOperator._run_cdc_process_for_table,
                    args=(self.full_load_plugin_name, full_load_object))
                full_load_jobs.append(full_load_process)
                full_load_process.start()

        # start CDC
        if self.load_type in [LoadType.CDC, LoadType.Full_Load_And_CDC]:
            cdc_init_params = {
                "output_folder_location": self.output_location,
                "connection_string": self.connection_string,
                "table_names": self.table_names,
            }
            cdc_object = self.cdc_plugin(**cdc_init_params)
            cdc_object.execute()

        # always wait for CDC processes to be completed
        for full_load_job in full_load_jobs:
            full_load_job.join()

    @staticmethod
    def _run_cdc_process_for_table(full_load_plugin_name: str, full_load_init_params: Dict) -> None:
        """
        Worker process for Full load process to complete
        :param full_load_plugin_name: plugin to be used
        :type full_load_plugin_name: str
        :param full_load_init_params:
        :type full_load_init_params:
        """
        full_load_plugin = next((sub_class for sub_class in CDCBase.__subclasses__()
                                 if sub_class.plugin_type == PlugInType.Full_Load
                                 and sub_class.plugin_name == full_load_plugin_name), None)

        full_load_object = full_load_plugin(**full_load_init_params)
        full_load_object.execute()
