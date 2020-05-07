import os
import psycopg2
import shutil

from siirto.plugins.full_load.full_load_base import FullLoadBase
from siirto.shared.enums import PlugInType


class PgDefaultFullLoadPlugin(FullLoadBase):
    """
    Postgres full load default plugin.

    :param split_file_size_limit: number of lines after which a
        new file will be created. Default is `1000000` lines.
    :type split_file_size_limit: int

    """

    # plugin type and plugin name
    plugin_type = PlugInType.Full_Load
    plugin_name = "PgDefaultFullLoadPlugin"
    plugin_parameters = {
        "split_file_size_limit": {
            "type": int
        }
    }

    def __init__(self,
                 split_file_size_limit: int = 1000000,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.split_file_size_limit = split_file_size_limit

    def _set_status(self, status):
        self.status = status

    def execute(self):
        self._set_status("in progress - started")
        connection = psycopg2.connect(self.connection_string)
        cursor = connection.cursor()
        # copy_query = f"\\COPY {self.table_name} TO program 'split -dl 1000000 " \
        #              f"--a _{self.table_name}.csv' (format csv)"
        shutil.rmtree(self.output_folder_location)
        os.makedirs(self.output_folder_location)
        file_to_write = os.path.join(self.output_folder_location, f"{self.table_name}_full.csv")
        with open(file_to_write, "w") as output_file:
            cursor.copy_to(output_file, self.table_name)
        self._set_status("in progress - bulk file created")
        if os.stat(file_to_write).st_size == 0:
            self._set_status("completed")
            self.rename_file(file_to_write)
            if self.notify_on_completion is not None:
                self.notify_on_completion(
                    **{
                        'status': 'success',
                        'table_name': self.table_name,
                        'error': "Table was empty"
                    }
                )
            return

        split_command = f'cd {self.output_folder_location} && split ' \
                        f'-dl {self.split_file_size_limit} {file_to_write} ' \
                        f'--a _{self.table_name}.csv'
        os.system(split_command)
        self._set_status("in progress - smaller files created")
        self.rename_file(file_to_write)
        self._set_status("completed")
        if self.notify_on_completion is not None:
            self.notify_on_completion(
                **{
                    'status': 'success',
                    'table_name': self.table_name,
                    'error': None
                }
            )

    def rename_file(self, file_to_write):
        if len(os.listdir(self.output_folder_location)) == 1:
            new_file_name = os.path.join(self.output_folder_location, f"x00_{self.table_name}.csv")
            os.rename(file_to_write, new_file_name)
        else:
            os.remove(file_to_write)
