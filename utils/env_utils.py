import os


def read_env_variable(key):
    value = os.environ.get(key)
    if value is None:
        print(f"Please set the environment variable {key}")
        exit(0)
    return value
