import signal
import sys
import psutil
import multiprocessing
from typing import Dict
import logging

from siirto.configuration import configuration
from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.shared.enums import DatabaseOperatorType, LoadType
from siirto.logger import create_rotating_log


def _validate_configuration_parameters():
    pass


def initialize():
    """ Initialize the lineage settings """
    # register interrupt signal
    register_ctrl_c_signal()

    # create the logger
    create_rotating_log()
    database_operator_name = configuration.get("conf", "database_operator")
    database_operator_type = configuration.get("conf", "database_operator_type")
    connection_string = configuration.get("conf", "connection_string")
    load_type = configuration.get("conf", "load_type")
    full_load_plugin_name = configuration.get("conf", "full_load_plugin_name")
    cdc_plugin_name = configuration.get("conf", "cdc_plugin_name")
    table_names_str = configuration.get("conf", "table_names", default="")
    table_names = table_names_str.split(",") if table_names_str.strip() != "" else []
    output_location = configuration.get("conf", "output_location")

    BaseDataBaseOperator.load_derived_classes()
    FullLoadBase.load_derived_classes()
    CDCBase.load_derived_classes()

    print(database_operator_name,
          database_operator_type,
          load_type,
          full_load_plugin_name,
          cdc_plugin_name,
          table_names_str,
          table_names,
          output_location)

    database_operator = BaseDataBaseOperator.get_object(database_operator_type, database_operator_name)

    database_operator_params = {
        "connection_string": connection_string,
        "load_type": LoadType[load_type],
        "table_names": table_names,
        "full_load_plugin_name": full_load_plugin_name,
        "cdc_plugin_name": cdc_plugin_name,
        "output_location": output_location,
    }

    retry_times = int(configuration.get("conf", "retry_times", 0))
    run_database_operator(database_operator,
                          database_operator_params,
                          retry_times)


def run_database_operator(database_operator: BaseDataBaseOperator,
                          database_operator_params: Dict,
                          retry_times: int) -> None:
    """
    run the actual database operator along with the configuration
    :param database_operator: database operator class
    :type database_operator: BaseDataBaseOperator
    :param database_operator_params: constructor params for the
        database parameters
    :type database_operator_params: Dict
    :param retry_times: number of time code should retry
    :type retry_times: int
    """
    logger = logging.getLogger("siirto")
    try:
        database_operator_object = database_operator(**database_operator_params)
        database_operator_object.execute()
    except Exception as exception:
        logger.error(f"Error occurred: {exception}")
        retry_times -= 1
        if retry_times >= 0:
            logger.error(f"{retry_times} times left. Trying one more times")
            run_database_operator(database_operator,
                                  database_operator_params,
                                  retry_times)


def register_ctrl_c_signal() -> None:
    """
    capture to control the ctrl+c signal
    run some clean up code here for graceful termination
    """
    def signal_handler(sig, frame):
        print(f'Interruption detected. process name: {multiprocessing.current_process().name}')
        siirto_processes = [p for p in psutil.process_iter() if p.name().startswith("siirto_")]
        print([p.name() for p in siirto_processes])
        for process in siirto_processes:
            process.terminate()

        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
