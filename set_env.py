import os


def set_env_from_env_file(filename='.env'):
    with open(filename, 'r') as f:
        for line in f:
            name, val = line.rstrip('\n').split('=')
            os.environ[name] = val
