import psycopg2
import os
from typing import Dict
import multiprocessing
import importlib

from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.shared.enums import DatabaseOperatorType, PlugInType, LoadType


class PostgresOperator(BaseDataBaseOperator):
    """
    Postgres default operator implements BaseDataBaseOperator
    """

    operator_type = DatabaseOperatorType.Postgres
    operator_name = "Postgres-Default"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.full_load_plugin = FullLoadBase.get_object(self.full_load_plugin_name)
        self.cdc_plugin = CDCBase.get_object(self.cdc_plugin_name)
        self._validate_input_parameters()

    def _validate_input_parameters(self):
        if self.cdc_plugin is None \
                and self.load_type in [LoadType.CDC, LoadType.Full_Load_And_CDC]:
            raise ValueError(f"Incorrect value "
                             f"provided for cdc plugin `{self.cdc_plugin_name}`")
        if self.full_load_plugin is None \
                and self.load_type in [LoadType.Full_Load, LoadType.Full_Load_And_CDC]:
            raise ValueError(f"Incorrect value "
                             f"provided for full load plugin `{self.full_load_plugin_name}`")

    @staticmethod
    def on_full_load_completed(status: str, table_name: str, error: str = None):
        if status == "success":
            print(f"Full load completed for {table_name}")
        else:
            print(f"Full load failed for {table_name} with error {error}")

    def execute(self):

        full_load_jobs = []

        # start full load
        # cdc for each table will run in new process
        if self.load_type in [LoadType.Full_Load, LoadType.Full_Load_And_CDC]:
            for table_name in self.table_names:
                full_load_output_location_of_table = os.path.join(self.output_location,
                                                                  "full_load")
                if not os.path.exists(full_load_output_location_of_table):
                    os.mkdir(full_load_output_location_of_table)
                output_location_of_table = os.path.join(full_load_output_location_of_table,
                                                        table_name.replace(".", "_"))
                if not os.path.exists(output_location_of_table):
                    os.mkdir(output_location_of_table)

                full_load_init_params = {
                    "output_folder_location": output_location_of_table,
                    "connection_string": self.connection_string,
                    "table_name": table_name,
                    "notify_on_completion": PostgresOperator.on_full_load_completed
                }
                full_load_process = multiprocessing.Process(
                    target=PostgresOperator._run_full_load_process_for_table,
                    args=(self.full_load_plugin_name, full_load_init_params))
                full_load_jobs.append(full_load_process)
                full_load_process.start()

        # start CDC
        # this will run on main thread
        if self.load_type in [LoadType.CDC, LoadType.Full_Load_And_CDC]:
            output_location_for_cdc = os.path.join(self.output_location,
                                                   "cdc")
            if not os.path.exists(self.output_location):
                os.mkdir(self.output_location)
            if not os.path.exists(output_location_for_cdc):
                os.mkdir(output_location_for_cdc)
            cdc_init_params = {
                "output_folder_location": output_location_for_cdc,
                "connection_string": self.connection_string,
                "table_names": self.table_names,
            }
            cdc_object = self.cdc_plugin(**cdc_init_params)
            cdc_object.execute()

        # always wait for CDC processes to be completed
        for full_load_job in full_load_jobs:
            full_load_job.join()

    @staticmethod
    def _run_full_load_process_for_table(full_load_plugin_name: str, full_load_init_params: Dict) -> None:
        """
        Worker process for Full load process to complete
        :param full_load_plugin_name: plugin to be used
        :type full_load_plugin_name: str
        :param full_load_init_params:
        :type full_load_init_params:
        """
        full_load_plugin = FullLoadBase.get_object(full_load_plugin_name)
        full_load_object = full_load_plugin(**full_load_init_params)
        full_load_object.execute()
