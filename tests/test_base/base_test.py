import unittest

from tests.test_definitions import *
from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.plugins.cdc.cdc_base import CDCBase
from siirto.plugins.full_load.full_load_base import FullLoadBase


class BaseTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        # setup the SIIRTO_CONFIG env variable for configuration to be read
        configuration_file_path_ = os.path.join(ROOT_TEST_DIR, "..", "configuration.cfg")
        os.environ["SIIRTO_CONFIG"] = configuration_file_path_
        # load all derived classes for the test setup
        CDCBase.load_derived_classes()
        FullLoadBase.load_derived_classes()
        BaseDataBaseOperator.load_derived_classes()

    @classmethod
    def tearUpClass(cls) -> None:
        # remove the SIIRTO_CONFIG env variable
        os.environ["SIIRTO_CONFIG"] = None

