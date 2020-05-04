from siirto.database_operators.base_database_operator import BaseDataBaseOperator
from siirto.shared.enums import DatabaseOperatorType


class PostgresOperator(BaseDataBaseOperator):
    """
    Postgres default operator implements BaseDataBaseOperator
    """

    operator_type = DatabaseOperatorType.Postgres
    name = "Postgres-Default"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self):
        # lopad plugins
        pass
