"""Exceptions used by Siirto"""


class SiirtoException(Exception):
    """
    Base class for all Siirto's errors.
    Each custom exception should be derived from this class
    """
    status_code = 500
