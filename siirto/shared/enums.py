from enum import Enum, unique


@unique
class LoadType(Enum):
    """
    Enum to select the nature of the job.
    Weather full load is required or/and CDC is required
    or both are required
    """
    Full_Load = 1
    CDC = 2
    Full_Load_And_CDC = 3


@unique
class DatabaseOperatorType(Enum):
    """
    Enum defining database operator type
    """
    Postgres = 1


@unique
class PlugInType(Enum):
    """
    Enum defining Plugin types
    """
    Full_Load = 1
    CDC = 2
