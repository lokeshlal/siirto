import os

ROOT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(ROOT_TEST_DIR, "..", "configuration.cfg")
os.environ["SIIRTO_CONFIG"] = configuration_file_path
