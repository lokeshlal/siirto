import shutil
import os
import unittest
import psycopg2
import testing.postgresql

from tests.test_definitions import *
from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.plugins.full_load.full_load_base import FullLoadBase


postgres = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)


# noinspection PyPep8Naming
def tearDownModule(self):
    # clear cached database at end of tests
    postgres.clear_cache()


class BaseTest(unittest.TestCase):

    def setUp(self) -> None:
        output_folder = os.environ["output_folder_path"]
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)

    def tearDown(self) -> None:
        output_folder = os.environ["output_folder_path"]
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)

    @classmethod
    def setUpClass(cls) -> None:
        # setup postgres db
        cls.setup_postgres()

        # setup the SIIRTO_CONFIG env variable for configuration to be read
        configuration_file_path_ = os.path.join(ROOT_TEST_DIR, "..", "configuration.cfg")
        os.environ["SIIRTO_CONFIG"] = configuration_file_path_
        # load all derived classes for the test setup
        CDCBase.load_derived_classes()
        FullLoadBase.load_derived_classes()
        BaseDataBaseOperator.load_derived_classes()

    @classmethod
    def setup_postgres(cls):
        cls.postgres = postgres()
        cls.postgres_connection_string = cls.postgres.url()
        cls.insert_data_in_postgres()

    @classmethod
    def insert_data_in_postgres(cls):
        # insert test data
        with psycopg2.connect(cls.postgres_connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('CREATE TABLE employee (id INT PRIMARY KEY, name VARCHAR(50));')

    @classmethod
    def tearUpClass(cls) -> None:
        # remove the SIIRTO_CONFIG env variable
        os.environ["SIIRTO_CONFIG"] = None
        # shutdown postgres
        cls.postgres.stop()
