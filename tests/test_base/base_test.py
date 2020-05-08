import shutil
import os
import unittest
import psycopg2
import testing.postgresql
import logging

from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.plugins.full_load.full_load_base import FullLoadBase


class BaseTest(unittest.TestCase):

    def setUp(self) -> None:
        self.output_folder = os.environ["output_folder_path"]
        if not os.path.exists(self.output_folder):
            os.mkdir(self.output_folder)

    def tearDown(self) -> None:
        output_folder = os.environ["output_folder_path"]
        if os.path.exists(output_folder):
            shutil.rmtree(output_folder)

    @classmethod
    def setUpClass(cls) -> None:
        logging.disable(logging.CRITICAL)
        cls.postgres_testing = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)
        # setup postgres db
        cls.setup_postgres()
        # load all derived classes for the test setup
        CDCBase.load_derived_classes()
        FullLoadBase.load_derived_classes()
        BaseDataBaseOperator.load_derived_classes()

    @classmethod
    def setup_postgres(cls):
        cls.postgres = cls.postgres_testing()
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
        logging.disable(logging.NOTSET)
        # remove the SIIRTO_CONFIG env variable
        os.environ["SIIRTO_CONFIG"] = None
        # shutdown postgres
        cls.postgres.stop()
        cls.postgres_testing.clear_cache()
