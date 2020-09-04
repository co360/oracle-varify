import os
import logging
from configparser import ConfigParser
from ..common.oracle_client import OracleDB
from ..common.common import get_data_from_oracle_config_ini
from ..models import SqliteDB


class GetOracleTablePrimaryKey:
    """ get oracle table primary key """

    def __init__(self):
        self.sqlite_db = SqliteDB()
        self.__table_data_config()
        self.__get_verify_tables()

    def __table_data_config(self):
        """ config table data """
        self.sqlite_db.oracle_table_data_tables_drop()
        self.sqlite_db.oracle_table_data_tables_create()
        self.config_dvt = get_data_from_oracle_config_ini('dvt')
        self.source_oracle_db = self.__oracle_login_init('source')
        # self.dest_oracle_db = self.__oracle_login_init('dest')

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

    def __get_verify_tables(self):
        tables = self.sqlite_db.sqlite_oracle_verify_each_object_table_query(
            'table', 'True')
        for table in tables:
            owner = table[0]
            table_name = table[1]
            self.__get_table_primary_key(owner, table_name)

    def __get_table_primary_key(self, owner: str, table_name: str):
        """ get table primary key """
        primary_keys = self.source_oracle_db.get_oracle_table_primary_key(
            owner, table_name)
        status = bool(primary_keys)
        self.sqlite_db.sqlite_oracle_table_primary_table_insert({
            'owner': owner,
            'table_name': table_name,
            'primary_status': str(status),
            'primary_keys': ','.join(primary_keys) if status else '__empty__'
        })
        logging.info(f'{owner, primary_keys}')
