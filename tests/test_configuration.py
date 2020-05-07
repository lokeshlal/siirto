from tests.test_base.base_test import BaseTest
from siirto.configuration import configuration


class TestConfiguration(BaseTest):

    def test_configuration(self):
        self.assertEqual(configuration.get("conf", "database_operator"), "Postgres-Default")
        self.assertEqual(configuration.get("conf", "database_operator_type"), "Postgres")
        self.assertEqual(configuration.get("conf", "connection_string"),
                         "host=127.0.0.1 dbname=repl user=ubuntu password=ubuntu")
        self.assertEqual(configuration.get("conf", "load_type"), "Full_Load_And_CDC")
        self.assertEqual(configuration.get("conf", "full_load_plugin_name"),
                         "PgDefaultFullLoadPlugin")
        self.assertEqual(configuration.get("conf", "cdc_plugin_name"), "PgDefaultCDCPlugin")
        self.assertEqual(configuration.get("conf", "table_names"), "public.employees")
        self.assertEqual(configuration.get("conf", "output_location"), "/mnt/d/delete_me")
        configuration.set("conf", "new_configuration_param", "new value")
        self.assertEqual(configuration.get("conf", "new_configuration_param"), "new value")

    def test_configuration_failed(self):
        self.assertEqual(configuration.get("conf", "non_existing_keys"), None)
        self.assertEqual(configuration.get("conf", "non_existing_keys", "NEW_VALUE"), "NEW_VALUE")
        self.assertEqual(configuration.get("conf", "non_existing_keys", 100), 100)
        self.assertEqual(configuration.get("conf", "non_existing_keys", {"key": "value"}),
                         {"key": "value"})
