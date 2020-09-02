from configparser import ConfigParser
import os
import logging


def get_data_from_ini_file(file_path, container_name, key):
    """
    read data from ini file
    file_path: ini file path
    container_name: ini [name]
    key: ini key
    return: str
    """
    config = ConfigParser()
    cur_dir = os.path.dirname(os.path.abspath('__file__'))
    config_path = os.path.join(cur_dir, file_path)
    if not os.path.exists(config_path):
        logging.error(f'{config_path} is not exist, please check')
        return False

    config.read(config_path)
    oracle_sql = config[container_name]
    if key in oracle_sql:
        return oracle_sql.get(key)
    else:
        logging.error(f'Target key is not exist')
        return False
