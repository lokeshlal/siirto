from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.shared.enums import PlugInType
from tests.test_base.base_test import BaseTest


class TestCDCBase(BaseTest):

    def test_cdc_base_plugin_parameters(self):
        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_names": ["public.employee"]
        }
        cdc_object = CDCBase(**cdc_init_params)

        self.assertEqual(list(cdc_object.__dict__.keys()).sort(),
                         [
                             "connection_string",
                             "output_folder_location",
                             "table_names",
                             "configuration",
                             "logger"
                         ].sort())

    def test_cdc_base_plugin_name_and_type(self):
        self.assertEqual(CDCBase.plugin_name, None)
        self.assertEqual(CDCBase.plugin_type, PlugInType.CDC)

    def test_cdc_base_plugin_empty_connection(self):
        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": None,
            "table_names": []
        }
        with self.assertRaises(ValueError) as cm:
            CDCBase(**cdc_init_params)
        self.assertEqual(str(cm.exception), "Connection string is None")

    def test_cdc_base_plugin_empty_output(self):
        cdc_init_params = {
            "output_folder_location": None,
            "connection_string": self.postgres_connection_string,
            "table_names": []
        }
        with self.assertRaises(ValueError) as cm:
            CDCBase(**cdc_init_params)
        self.assertEqual(str(cm.exception), "output_folder_location is empty")

    def test_cdc_base_plugin_empty_table(self):
        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_names": None
        }
        with self.assertRaises(ValueError) as cm:
            CDCBase(**cdc_init_params)
        self.assertEqual(str(cm.exception), "Table name is None or Empty")
        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_names": "public.employee"
        }
        with self.assertRaises(ValueError) as cm:
            CDCBase(**cdc_init_params)
        self.assertEqual(str(cm.exception), "Table name is None or Empty")

    def test_cdc_base_plugin_execute(self):
        cdc_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_names": []
        }
        with self.assertRaises(NotImplementedError) as cm:
            cdc_base_object = CDCBase(**cdc_init_params)
            cdc_base_object.execute()

    def test_cdc_base_plugin_load_cdc_default_plugin(self):
        CDCBase.load_derived_classes()
        child_operator_object = CDCBase.get_object("PgDefaultCDCPlugin")
        self.assertEqual(child_operator_object.__dict__["__module__"],
                         "siirto.plugins.cdc.pg_default_cdc_plugin")
        self.assertEqual(child_operator_object.__name__, "PgDefaultCDCPlugin")
