from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.shared.enums import PlugInType
from tests.test_base.base_test import BaseTest


class TestCDCBase(BaseTest):

    def test_full_load_base_plugin_name_and_type(self):
        self.assertEqual(FullLoadBase.plugin_name, None)
        self.assertEqual(FullLoadBase.plugin_type, PlugInType.Full_Load)

    def test_full_load_base_plugin_empty_connection(self):
        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": None,
            "table_name": "public.employee",
            "notify_on_completion": None
        }
        with self.assertRaises(ValueError) as cm:
            FullLoadBase(**full_load_init_params)
        self.assertEqual(str(cm.exception), "Connection string is None")

    def test_full_load_base_plugin_empty_output(self):
        full_load_init_params = {
            "output_folder_location": None,
            "connection_string": self.postgres_connection_string,
            "table_name": "public.employee",
            "notify_on_completion": None
        }
        with self.assertRaises(ValueError) as cm:
            FullLoadBase(**full_load_init_params)
        self.assertEqual(str(cm.exception), "output_folder_location is empty")

    def test_full_load_base_plugin_empty_table(self):
        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": None,
            "notify_on_completion": None
        }
        with self.assertRaises(ValueError) as cm:
            FullLoadBase(**full_load_init_params)
        self.assertEqual(str(cm.exception), "Table name is None")
        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": "",
            "notify_on_completion": None
        }
        with self.assertRaises(ValueError) as cm:
            FullLoadBase(**full_load_init_params)
        self.assertEqual(str(cm.exception), "Table name is None")

    def test_full_load_base_plugin_execute(self):
        full_load_init_params = {
            "output_folder_location": self.output_folder,
            "connection_string": self.postgres_connection_string,
            "table_name": "public.employee",
            "notify_on_completion": None
        }
        with self.assertRaises(NotImplementedError) as cm:
            full_load_base_object = FullLoadBase(**full_load_init_params)
            full_load_base_object.execute()

    def test_full_load_base_plugin_load_cdc_default_plugin(self):
        FullLoadBase.load_derived_classes()
        child_operator_object = FullLoadBase.get_object("PgDefaultFullLoadPlugin")
        self.assertEqual(child_operator_object.__dict__["__module__"],
                         "siirto.plugins.full_load.pg_default_full_load_plugin")
        self.assertEqual(child_operator_object.__name__, "PgDefaultFullLoadPlugin")
