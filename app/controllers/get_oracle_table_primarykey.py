import os
import logging
from configparser import ConfigParser
from ..common.oracle_client import OracleDB
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

    def __get_verify_tables(self):
        tables = self.sqlite_db.sqlite_oracle_verify_each_object_table_query(
            'table', 'True')
        for table in tables:
            owner = table[0]
            table_name = table[1]
            self.__get_table_primary_key(owner, table_name)

    def __get_table_primary_key(self, owner, table_name):
        """ get table primary key """
        logging.info(f'{owner, table_name}')
