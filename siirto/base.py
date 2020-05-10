import logging

from siirto.configuration import configuration


class Base:
    """
    Base class for all classes in Siirto.
    Provide basic properties
        1. self.configuration
        2. self.logger
    """
    def __init__(self,
                 *args,
                 **kwargs):
        self.configuration = configuration
        self.logger = logging.getLogger("siirto")
