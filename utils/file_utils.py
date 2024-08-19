import os
import pandas as pd
from config import *
import glob


def create_output_directories():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    if not os.path.exists(RESULTS_DIR):
        os.makedirs(RESULTS_DIR)


def store_csv(directory, file_name, df):
    if df is None:
        return
    path = os.path.join(directory, file_name)
    if os.path.exists(directory):
        df.to_csv(path)


def load_csv(directory, file_name):
    path = os.path.join(directory, file_name)
    if not os.path.exists(path):
        print(f"Path does not exist: {path}")
        return None
    df = pd.read_csv(path)
    return df


def delete_file(directory, file_name):
    path = os.path.join(directory, file_name)
    if os.path.exists(path):
        os.remove(path)


def get_os_variable(key):
    if key in os.environ:
        return os.environ[key]
    else:
        print(f"{key} not set as OS environment variable - exiting")
        exit(0)


def delete_files_in_directory(dir_path):
    # Get all files in the directory
    files = glob.glob(os.path.join(dir_path, "*"))

    for file_path in files:
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")