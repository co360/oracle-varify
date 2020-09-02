import openpyxl
import logging
import os
from ..common.common import get_all_data_from_ini_file

class ExportOracleObjectsStaticticExcel:
    """
    export oracle collect data to excel
    """

    def __init__(self):
        self.__config_init()
    
    def __config_init(self):
        ini_path = os.path.join('app', 'models.ini')
        ini_container = 'oracle_table_name'
        self.oracle_table_data = get_all_data_from_ini_file(ini_path, ini_container)
        logging.info(f'{self.oracle_table_data}')