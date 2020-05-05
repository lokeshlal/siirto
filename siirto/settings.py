from siirto.configuration import configuration
from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.shared.enums import DatabaseOperatorType, LoadType


def validate_configuration_parameters():
    pass


def initialize():
    """ Initialize the lineage settings """
    database_operator_name = configuration.get("conf", "database_operator")
    database_operator_type = configuration.get("conf", "database_operator_type")
    connection_string = configuration.get("conf", "connection_string")
    load_type = configuration.get("conf", "load_type")
    full_load_plugin_name = configuration.get("conf", "full_load_plugin_name")
    cdc_plugin_name = configuration.get("conf", "cdc_plugin_name")
    table_names_str = configuration.get("conf", "table_names", default="")
    table_names = table_names_str.split(",")
    output_location = configuration.get("conf", "output_location")

    database_operator = next((sub_class for sub_class in BaseDataBaseOperator.__subclasses__()
                              if sub_class.operator_type == DatabaseOperatorType[database_operator_type]
                              and sub_class.operator_name == database_operator_name), None)

    database_operator_params = {
        "connection_string": connection_string,
        "load_type": LoadType[load_type],
        "table_names": table_names,
        "full_load_plugin_name": full_load_plugin_name,
        "cdc_plugin_name": cdc_plugin_name,
        "output_location": output_location,
    }
    database_operator_object = database_operator(**database_operator_params)
    database_operator_object.execute()
