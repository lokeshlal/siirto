import os
from typing import Any
from configparser import ConfigParser
from siirto.exceptions import SiirtoException


class SiirtoConfiguration:
    """
    Siirto configuration module.
    Loads the config file and provide it to various modules

    :param filename: configuration file path
    :type filename: str
    """

    def __init__(self,
                 filename: str) -> None:
        configuration_text = None
        if filename is None \
                or len(filename.strip()) == 0 \
                or not os.path.isabs(filename):
            raise SiirtoException("configuration file path is missing. "
                                  "Please set SIIRTO_CONFIG env variable.")

        if not os.path.isabs(filename):
            # filename in this case is configuration_text
            configuration_text = filename

        if configuration_text is None:
            if os.path.exists(filename):
                with open(filename, "r") as config_file:
                    configuration_text = config_file.read()
            else:
                raise SiirtoException(f"Configuration file is not present at {filename}")

        if configuration_text is not None:
            self.configuration_parser = ConfigParser()
            self.configuration_parser.read_string(configuration_text)
        else:
            raise SiirtoException(f"Not able to read Configuration file at "
                                  f"{filename} or file empty")

    def get(self,
            section: str,
            key: str,
            default: Any = None):
        """
        Get the value from configuration object
        :param section: section
        :type section: str
        :param key: key
        :type key: str
        :param default: default value, if section->key not found
        :type default: Any
        :return: section -> key value from config file
        """
        if self.configuration_parser.has_option(section, key):
            return self.configuration_parser.get(section, key)
        return default

    def set(self,
            section: str,
            key: str,
            value: Any = None) -> None:
        """
        Set the section->key value in configuration object
        :param section: section
        :type section: str
        :param key: key
        :type key: str
        :param value: default value, if section->key not found
        :type value: Any
        """
        self.configuration_parser.set(section, key, value)

    def get_user_defined(self, key, default=None):
        """
        Get the value from configuration object from `user_defined section`
        :param key: key
        :type key: str
        :param default: default value, if section->key not found
        :type default: Any
        :return: "user_defined" sectioin -> key value from config file
        """
        return self.get("user_defined", key, default)


configuration = SiirtoConfiguration(os.environ.get("SIIRTO_CONFIG"))
