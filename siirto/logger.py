import logging
import os
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


def create_rotating_log():
    """
    Creates a rotating log
    :param path: path for the logs
    """
    logger = logging.getLogger("Siirto")
    logger.setLevel(logging.INFO)
    path = configuration.get("logs",
                             "log_file_path",
                             "/var/log/siirto")
    _create_path(path)
    log_file_path = os.path.join(path, "siirto.log")
    # add a rotating handler
    handler = RotatingFileHandler(log_file_path,
                                  maxBytes=configuration.get("logs",
                                                             "log_file_max_bytes",
                                                             10485760),
                                  backupCount=configuration.get("logs",
                                                                "log_file_backup_count",
                                                                10))
    logger.addHandler(handler)
