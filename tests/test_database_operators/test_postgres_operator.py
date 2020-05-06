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
        with self.assertRaises(ValueError) as cm:
            database_operator_params = {
                "connection_string": "connection_string",
                "load_type": LoadType.Full_Load_And_CDC,
                "table_names": ['public.employees'],
                "full_load_plugin_name": "PgDefaultFullLoadPlugin",
                "cdc_plugin_name": "PgDefaultCDCPlugin_InCorrect",
                "output_location": "/mnt/d/delete_me",
            }
            b = PostgresOperator(**database_operator_params)
        self.assertEqual(str(cm.exception), "Incorrect value provided for "
                                            "cdc plugin `PgDefaultCDCPlugin_InCorrect`")
