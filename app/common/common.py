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
    logging.info(f'========{config_path}')
    config.read(config_path)
    container_sql = config[container_name]
    if key in container_sql:
        return container_sql.get(key)
    else:
        logging.error(f'Target key is not exist')
        return False


def get_all_data_from_ini_file(file_path, container_name):
    """
    read all container data from ini file
    file_path: ini file path
    container_name: ini [name]
    return: dict
    """
    config = ConfigParser()
    cur_dir = os.path.dirname(os.path.abspath('__file__'))
    config_path = os.path.join(cur_dir, file_path)
    if not os.path.exists(config_path):
        logging.error(f'{config_path} is not exist, please check')
        return False

    logging.info(f'========{config_path}')
    config.read(config_path)
    if container_name in config:
        result = config[container_name]
        return dict(result)
    else:
        logging.error(f'{container_name} is not exist in config.ini')
        return False
