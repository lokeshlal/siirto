import unittest
import sys
from tests.test_base.base_test import BaseTest


def suite():
    """Test suite for siirto project"""
    test_suite = unittest.TestSuite()
    test_suite_classes = []
    for sub_class in BaseTest.__subclasses__():
        test_suite_classes.append(unittest.TestLoader().loadTestsFromTestCase(sub_class))

    test_suite.addTests(test_suite_classes)
    return test_suite


if __name__ == '__main__':
    test_runner = unittest.TextTestRunner()
    test_runner.run(suite())
