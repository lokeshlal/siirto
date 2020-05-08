import multiprocessing
import os
import time
from typing import Dict, Any, List
from datetime import datetime
import uuid

from siirto.configuration import configuration
from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.logger import create_rotating_log
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.shared.enums import DatabaseOperatorType, LoadType


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
            ex_msg = f"Incorrect value provided " \
                     f"for cdc plugin `{self.cdc_plugin_name}`"
            self.logger.exception(ex_msg)
            raise ValueError(ex_msg)
        if self.full_load_plugin is None \
                and self.load_type in [LoadType.Full_Load, LoadType.Full_Load_And_CDC]:
            ex_msg = f"Incorrect value provided for " \
                     f"full load plugin `{self.full_load_plugin_name}`"
            self.logger.exception(ex_msg)
            raise ValueError(ex_msg)

    # @staticmethod
    def on_full_load_completed(self, status: str, table_name: str, error: str = None):
        if status == "success":
            self.logger.info(f"Full load completed for {table_name}")
        else:
            self.logger.error(f"Full load failed for {table_name} with error {error}")

    def execute(self):
        self.logger.info(f"Process started at {datetime.now()}")
        full_load_jobs = []

        # start full load
        # cdc for each table will run in new process
        full_load_jobs = self._process_full_load(full_load_jobs)

        # start CDC
        # this will run on main thread
        cdc_process = self._process_cdc()

        while True:
            # sleep for 2 seconds, before next check
            time.sleep(2)
            processes_running = False
            for full_load_job in full_load_jobs:
                if full_load_job.is_alive():
                    processes_running = True
            if cdc_process is not None and cdc_process.is_alive():
                processes_running = True
            if not processes_running:
                break
            # if asked to terminate the process from outside
            if self.terminate_process_signal:
                for full_load_job in full_load_jobs:
                    if full_load_job.is_alive():
                        full_load_job.terminate()
                if cdc_process is not None and cdc_process.is_alive():
                    cdc_process.terminate()

    def _process_cdc(self):
        """
        Process the CDC workloads
        """
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
            cdc_process = multiprocessing.Process(
                target=PostgresOperator._run_cdc_process,
                args=(self.cdc_plugin_name, cdc_init_params))
            cdc_process.name = "siirto_cdc_" + str(uuid.uuid4())
            cdc_process.start()
            return cdc_process
        return None

    def _process_full_load(self, full_load_jobs: List[Any]):
        """
        Process the full load jobs.
        One process/job for every table.
        :param full_load_jobs: captures the processes
        :type full_load_jobs: List
        :return: full_load_jobs
        """
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
                    "notify_on_completion": self.on_full_load_completed
                }
                full_load_process = multiprocessing.Process(
                    target=PostgresOperator._run_full_load_process_for_table,
                    args=(self.full_load_plugin_name, full_load_init_params))
                full_load_process.name = "siirto_full_load_" + str(uuid.uuid4())
                full_load_jobs.append(full_load_process)
                full_load_process.start()
        return full_load_jobs

    @staticmethod
    def _run_cdc_process(cdc_plugin_name: str,
                         cdc_init_params: Dict) -> None:
        """
        Worker process for cdc process to complete
        :param cdc_plugin_name: plugin to be used
        :type cdc_plugin_name: str
        :param cdc_init_params:
        :type cdc_init_params:
        """

        cdc_plugin = CDCBase.get_object(cdc_plugin_name)
        plugin_parameters = cdc_plugin.plugin_parameters
        cdc_init_params = \
            PostgresOperator.append_plugin_parameter_from_configuration(
                cdc_init_params,
                plugin_parameters
            )
        create_rotating_log("cdc", "siirto.log", "siirto")
        cdc_object = cdc_plugin(**cdc_init_params)
        cdc_object.execute()

    @staticmethod
    def _run_full_load_process_for_table(full_load_plugin_name: str,
                                         full_load_init_params: Dict) -> None:
        """
        Worker process for Full load process to complete
        :param full_load_plugin_name: plugin to be used
        :type full_load_plugin_name: str
        :param full_load_init_params:
        :type full_load_init_params:
        """
        full_load_plugin = FullLoadBase.get_object(full_load_plugin_name)
        plugin_parameters = full_load_plugin.plugin_parameters
        full_load_init_params = \
            PostgresOperator.append_plugin_parameter_from_configuration(
                full_load_init_params,
                plugin_parameters
            )
        create_rotating_log(f"full_load/{full_load_init_params['table_name']}",
                            "siirto.log",
                            "siirto")
        full_load_object = full_load_plugin(**full_load_init_params)
        full_load_object.execute()

    @staticmethod
    def append_plugin_parameter_from_configuration(init_params, plugin_parameters):
        for plugin_parameter in plugin_parameters.keys():
            plugin_parameter_value = \
                configuration.get("plugin_parameter", plugin_parameter)
            if plugin_parameter_value:
                init_params[plugin_parameter] = \
                    plugin_parameters[plugin_parameter]["type"](plugin_parameter_value)
        return init_params
