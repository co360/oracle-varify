import openpyxl
import logging
import os
import hashlib
from ..common.common import get_all_data_from_ini_file
from ..models import SqliteDB


class ExportOracleObjectsStaticticExcel:
    """
    export oracle collect data to excel
    """

    def __init__(self, users):
        self.__config_init()
        self.__sqlite_db = SqliteDB()
        self.users = users
        self.__compare_users_init()

    def __compare_users_init(self):
        for user in self.users:
            self.__compare_user_objects_name_status(user)

    def __compare_user_objects_name_status(self, user):
        table_name = self.oracle_table_data['oracle_sequence']
        object_source_data = self.__sqlite_db.sqlite_common_table_query(
            user, table_name, 'source')
        object_dest_data = self.__sqlite_db.sqlite_common_table_query(
            user, table_name, 'dest')
        if object_source_data:
            logging.info(f'{object_source_data}')
            logging.info(f'{object_dest_data}')
            table_source_md5 = self.__generate_md5_hexdigest(object_source_data)
            table_dest_md5 = self.__generate_md5_hexdigest(object_dest_data)
            logging.info(f'source md5 {table_source_md5}')
            logging.info(f'source md5 {table_dest_md5}')
            if table_source_md5 == table_dest_md5:
                logging.info(f'{user} {table_name} equal')
            else:
                logging.error(f'{user} {table_name} is not equal')

    def __generate_md5_hexdigest(self, data):
        md5 = hashlib.md5()
        string_data = str(data)
        md5.update(string_data.encode('utf-8'))
        result = md5.hexdigest()
        return result

    def __config_init(self):
        ini_path = os.path.join('app', 'models.ini')
        ini_container = 'oracle_table_name'
        self.oracle_table_data = get_all_data_from_ini_file(
            ini_path, ini_container)
        logging.info(f'{self.oracle_table_data}')
