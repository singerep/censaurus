from importlib_resources import files
import censaurus._data

def state_ids_path():
    return files(censaurus._data).joinpath('state_ids.csv')