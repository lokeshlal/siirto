from siirto.shared.enums import LoadType
from tests.test_base.base_test import BaseTest
from siirto.database_operators.base_database_operator import BaseDataBaseOperator


class TestBaseDataBaseOperator(BaseTest):

    def test_base_database_operator_empty_table(self):
        with self.assertRaises(ValueError) as cm:
            b = BaseDataBaseOperator(
                self.postgres_connection_string,
                LoadType.Full_Load_And_CDC,
                [],
                "full_load_plugin_name",
                "cdc_plugin_name",
                self.output_folder
            )
        self.assertEqual(str(cm.exception), "Table names `[]` should not be empty")

    def test_base_database_operator_empty_connection(self):
        with self.assertRaises(ValueError) as cm:
            b = BaseDataBaseOperator(
                "",
                LoadType.Full_Load_And_CDC,
                ["table.employee"],
                "full_load_plugin_name",
                "cdc_plugin_name",
                self.output_folder
            )
        self.assertEqual(str(cm.exception), "Empty value provided for connection string")

    def test_base_database_operator_empty_output(self):
        with self.assertRaises(ValueError) as cm:
            b = BaseDataBaseOperator(
                self.postgres_connection_string,
                LoadType.Full_Load_And_CDC,
                ["public.employee"],
                "full_load_plugin_name",
                "cdc_plugin_name",
                ""
            )
        self.assertEqual(str(cm.exception), "Empty value provided for output location")

    def test_base_database_operator_empty_load_type(self):
        with self.assertRaises(ValueError) as cm:
            b = BaseDataBaseOperator(
                self.postgres_connection_string,
                None,
                ["public.employee"],
                "full_load_plugin_name",
                "cdc_plugin_name",
                self.output_folder
            )
        self.assertEqual(str(cm.exception), "Incorrect value provided for load type None")

    def test_base_database_operator_execute(self):
        with self.assertRaises(NotImplementedError) as cm:
            b = BaseDataBaseOperator(
                self.postgres_connection_string,
                LoadType.Full_Load_And_CDC,
                ["public.employee"],
                "full_load_plugin_name",
                "cdc_plugin_name",
                self.output_folder
            )
            b.execute()

    def test_base_database_operator_load_postgres_default_operator(self):
        BaseDataBaseOperator.load_derived_classes()
        child_operator_object = BaseDataBaseOperator.get_object("Postgres", "Postgres-Default")
        self.assertEqual(child_operator_object.__dict__["__module__"],
                         "siirto.database_operators.postgres_operator")
        self.assertEqual(child_operator_object.__name__, "PostgresOperator")
