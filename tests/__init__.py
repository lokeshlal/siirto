import os
import testing.postgresql as tp

# override the testing.postgresql to include
# wal_level=logical while starting the postgresql
tp.Postgresql.DEFAULT_SETTINGS = dict(auto_start=2,
                                      base_dir=None,
                                      initdb=None,
                                      initdb_args='-U postgres -A trust',
                                      postgres=None,
                                      postgres_args='-h 127.0.0.1 -F -c logging_collector=off -c '
                                                    'wal_level=logical',
                                      pid=None,
                                      port=None,
                                      copy_data_from=None)

ROOT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

configuration_file_path = os.path.join(ROOT_TEST_DIR, "..", "configuration.cfg")

output_folder_path = os.path.abspath(os.path.join(ROOT_TEST_DIR, "..", "test_output_folder"))
if not os.path.exists(output_folder_path):
    os.mkdir(output_folder_path)

os.environ["SIIRTO_CONFIG"] = configuration_file_path
os.environ["output_folder_path"] = output_folder_path
