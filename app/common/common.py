from configparser import ConfigParser
import os
import logging
import hashlib


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


def get_data_from_oracle_config_ini(container_name: str):
    """ read data from config.ini """
    ini_path = 'config.ini'
    cur_dir = os.path.dirname(os.path.abspath('__file__'))
    config_path = os.path.join(cur_dir, ini_path)
    result = get_all_data_from_ini_file(config_path, container_name)
    return result


def varify_data_use_md5(source, dest):
    """
    use md5 verify source and dest equal 
    source: str
    dest: str
    return True/False
    """
    md5_source = hashlib.md5()
    md5_dest = hashlib.md5()
    md5_source.update(str(source).encode('utf-8'))
    md5_dest.update(str(dest).encode('utf-8'))
    verify_souce = md5_source.hexdigest()
    verify_dest = md5_dest.hexdigest()

    if verify_souce == verify_dest:
        return True
    else:
        return False
