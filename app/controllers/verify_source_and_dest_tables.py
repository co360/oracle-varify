import os
import logging
from ..common.oracle_client import OracleDB
from ..models import SqliteDB
from ..common.common import get_data_from_oracle_config_ini


class VerifySourceAndDestTables:
    """ verify source and dest tables """

    def __init__(self):
        self.sqlite_db = SqliteDB()
        self.__table_data_config()
        self.__get_target_tables()

    def __table_data_config(self):
        """ config table data """
        logging.info('Start get Table primary key')
        self.sqlite_db.oracle_verify_source_dest_tables_drop()
        self.sqlite_db.oracle_verify_source_dest_tables_create()
        self.source_oracle_db = self.__oracle_login_init('source')
        self.dest_oracle_db = self.__oracle_login_init('dest')

    def __oracle_login_init(self, type: str):
        """ check oracle db status """
        oracle_db = {}
        if type == 'source' or type == 'dest':
            login = get_data_from_oracle_config_ini(type)
            oracle_db = OracleDB(login)
            oracle_db.connect_oracle
            oracle_db.env_init()
        else:
            logging.error('Please input valid type like source or dest')
            raise Exception("valid type, shoule like source/dest")
        return oracle_db

    def __get_target_tables(self):
        tables = self.sqlite_db.sqlite_oracle_table_column_table_query()
        logging.info(tables)
