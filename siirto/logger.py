import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from siirto.configuration import configuration


def _create_path(path):
    """
    create entire path till the log file folder
    :param path: path for the logs
    """
    if os.path.exists(path):
        return
    if not os.path.exists(str(Path(path).parent)):
        _create_path(str(Path(path).parent))
    os.mkdir(path)


def create_rotating_log(relative_path: str = "",
                        file_name: str = "siirto.log",
                        logger_name: str = "siirto",
                        path: str = None):
    """
    Creates a rotating log

    :param relative_path: relative path for the logger from the path
    :type relative_path: str
    :param file_name: file_name to be used
    :type file_name: str
    :param logger_name: name for the logger
    :type logger_name: str
    :param path: base path to be used
    :type path: str
    """
    logger = logging.getLogger(logger_name)
    log_formatter = configuration.get("logs",
                                      "log_formatter",
                                      "[%%(asctime)s] {%%(filename)s:%%(lineno)d} "
                                      "%%(levelname)s - %%(message)s")
    formatter = logging.Formatter(fmt=log_formatter,
                                  datefmt='%Y-%m-%d %H:%M:%S')


    if configuration.get("logs", "print_only_logs", "False") == "False":
        if not path:
            path = configuration.get("logs",
                                     "log_file_path",
                                     "/var/log/siirto")
        path = os.path.join(path, relative_path)
        _create_path(path)
        log_file_path = os.path.join(path, file_name)
        # add a rotating handler
        handler = RotatingFileHandler(log_file_path,
                                      maxBytes=int(configuration.get("logs",
                                                                     "log_file_max_bytes",
                                                                     "10485760")),
                                      backupCount=int(configuration.get("logs",
                                                                        "log_file_backup_count",
                                                                        "10")))
        handler.setFormatter(formatter)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

    logger.addHandler(handler)
    log_level_name = configuration.get("logs",
                                       "log_file_log_level",
                                       "DEBUG")
    log_level = getattr(logging, log_level_name)
    logging.basicConfig(level=log_level,
                        format=log_formatter)
