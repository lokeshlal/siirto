import logging

from siirto.configuration import configuration


class Base:
    """
    Base class for all classes in Siirto
    """
    def __init__(self,
                 *args,
                 **kwargs):
        self.configuration = configuration
        self.logger = logging.getLogger("siirto")
