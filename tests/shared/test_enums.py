from tests.test_base.base_test import BaseTest
from siirto.shared.enums import LoadType, DatabaseOperatorType, PlugInType


class TestEnums(BaseTest):

    def test_load_Type_enum(self):
        full_load = LoadType(1)
        self.assertEqual(full_load, LoadType.Full_Load)
        cdc = LoadType(2)
        self.assertEqual(cdc, LoadType.CDC)
        full_load_and_cdc = LoadType(3)
        self.assertEqual(full_load_and_cdc, LoadType.Full_Load_And_CDC)
        # there are only 3 valid load types
        value_in_load_type = list(map(lambda item: item.value, LoadType))
        # check for number of items
        self.assertEqual(len(value_in_load_type), 3)
        # check for duplicates
        self.assertEqual(len(value_in_load_type), len(set(value_in_load_type)))

    def test_database_operator_Type_enum(self):
        postgres = DatabaseOperatorType(1)
        self.assertEqual(postgres, DatabaseOperatorType.Postgres)
        # there are only 1 valid load types
        value_in_database_operator_type = list(map(lambda item: item.value, DatabaseOperatorType))
        # check for number of items
        self.assertEqual(len(value_in_database_operator_type), 1)
        # check for duplicates
        self.assertEqual(len(value_in_database_operator_type),
                         len(set(value_in_database_operator_type)))

    def test_plugin_Type_enum(self):
        full_load = PlugInType(1)
        self.assertEqual(full_load, PlugInType.Full_Load)
        cdc = PlugInType(2)
        self.assertEqual(cdc, PlugInType.CDC)
        # there are only 2 valid load types
        value_in_plugin_type = list(map(lambda item: item.value, PlugInType))
        # check for number of items
        self.assertEqual(len(value_in_plugin_type), 2)
        # check for duplicates
        self.assertEqual(len(value_in_plugin_type), len(set(value_in_plugin_type)))
