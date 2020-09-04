import os
import logging
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
        logging.info('Start get Table primary key')
        self.sqlite_db.oracle_table_data_tables_drop()
        self.sqlite_db.oracle_table_data_tables_create()
        self.config_dvt = get_data_from_oracle_config_ini('dvt')
        self.source_oracle_db = self.__oracle_login_init('source')
        self.dest_oracle_db = self.__oracle_login_init('dest')
        logging.info(self.config_dvt)

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
        logging.info('Start collect table columns, waitint!!!')
        tables = self.sqlite_db.sqlite_oracle_verify_each_object_table_query(
            'table', 'True')
        for table in tables:
            owner = table[0]
            table_name = table[1]
            self.__get_table_primary_key(owner, table_name)
        logging.info('Collect table column finished')

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

        status and self.__get_table_columns(owner, table_name, primary_keys)

    def __format_table_column(self, columns: dict, primary_keys: list):
        """ format columns and primary_keys """
        column_names = columns.keys

    def __filter_table_column(self, column: list):
        """ filter column """
        no_verify_str = self.config_dvt['no_verify_column_list']
        no_verify_list = [item.upper() for item in no_verify_str.split(',')]
        for index, column_item in enumerate(column, start=0):
            if column_item in no_verify_list:
                column.pop(index)
                logging.warning(f'filter column {column_item}')

    def __get_verify_percent(self, table_name: str):
        """ get table verify percent """
        verify_percent = float(self.config_dvt['verify_percent'])
        if not verify_percent:
            verify_percent = 1.0

        all_verify_table = self.config_dvt['all_verify_table']
        if all_verify_table:
            tables = all_verify_table.split(',')
            if table_name in tables:
                verify_percent = 1.0
        return verify_percent

    def __get_table_num_rows(self, owner: str, table_name: str):
        """ get table rows """

    def __get_table_columns(self, owner: str, table_name: str, primary_keys: list):
        """ get table column """
        table_column = self.source_oracle_db.get_oracle_table_column(
            owner, table_name)
        self.__filter_table_column(table_column)
        columns = ','.join(table_column.keys())
        primarys = ','.join(primary_keys)
        primary_type_list = [table_column[item] for item in primary_keys]
        primary_types = ','.join(primary_type_list)
        verify_percent = self.__get_verify_percent(table_name)
        source_num_rows = self.source_oracle_db.get_oracle_table_num_rows(
            owner, table_name)
        source_num_rows = str(source_num_rows) if source_num_rows else '0'
        dest_num_rows = self.source_oracle_db.get_oracle_table_num_rows(
            owner, table_name)
        dest_num_rows = str(dest_num_rows) if dest_num_rows else '0'
        logging.info(f'{source_num_rows, dest_num_rows}')
        num_rows = ','.join([source_num_rows, dest_num_rows])

        self.sqlite_db.sqlite_oracle_table_column_table__insert({
            'owner': owner,
            'table_name': table_name,
            'columns': columns,
            'num_rows': num_rows,
            'primarys': primarys,
            'primary_types': primary_types,
            'verify_percent': verify_percent
        })

        logging.info(
            f'{owner, columns, num_rows, primarys, primary_types, verify_percent}')
