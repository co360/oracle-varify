import os
import math
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
        self.default_per_page = 4
        self.last_page_count = 0
        self.cur_primary_value = ''
        # self.sqlite_db.oracle_verify_source_dest_tables_drop()
        # self.sqlite_db.oracle_verify_source_dest_tables_create()
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
        # logging.info(tables)
        for table in tables:
            pass
            # self.__analysis_target_table_data(table)
        self.__analysis_target_table_data(tables[1])

    def __get_table_max_rows(self, table_name: str, num_rows: str):
        """ get max_num """
        if ',' not in num_rows:
            logging.error(f'Num_rows {num_rows} error, should has ,')
            raise Exception(f'num_rows {num_rows} error')

        row_list = num_rows.split(',')
        source_rows = row_list[0]
        dest_rows = row_list[1]
        max_rows = source_rows if source_rows > dest_rows else dest_rows
        logging.info(f'Table {table_name} analysis num_rows is {max_rows}')
        return max_rows

    def __get_table_verify_rows(self, table_name: str, num_rows: int, verify_percent: str):
        """ get table pages """
        percent = float(verify_percent)
        if percent > 1:
            raise Exception(
                f'Table {table_name} verify_percent must little 1, current is {verify_percent}')
        result = math.ceil(int(num_rows) * percent)
        logging.info(f'Table {table_name} varify num_rows is {result}')
        return result

    def __get_table_per_pages(self, table_name: str, varify_rows: int):
        """ get table per pages """
        pages = 1
        last_page = 0

        if varify_rows < self.default_per_page:
            self.default_per_page = varify_rows
            self.last_page_count = 0
        else:
            pages = varify_rows // self.default_per_page
            self.last_page_count = varify_rows % self.default_per_page

        logging.info(
            f'Table {table_name} page info is {pages, self.default_per_page, self.last_page_count}')
        return pages

    def __analysis_target_table_data(self, table_data: list):
        """ analysis data get num_rows """
        table_name = table_data[1]
        max_rows = table_data[3]
        varify_percent = table_data[-1]
        logging.info(table_data)
        max_rows = self.__get_table_max_rows(table_name, max_rows)
        varify_rows = self.__get_table_verify_rows(
            table_name, max_rows, varify_percent)
        pages = self.__get_table_per_pages(table_name, varify_rows)

        for page in range(1, pages + 1):
            self.__query_oracle_table_order_by_primary(
                table_data, page)
        self.__query_oracle_table_order_by_primary(
            table_data, pages+1)

    def __format_table_query_sql(self, table_data: list, page: int):
        """ format table query sql """
        owner = table_data[0]
        table_name = table_data[1]
        columns = table_data[2]
        primarys = table_data[-2]
        primary_list = primarys.split(',')
        primary_sql_asc_list = [f'{item} ASC' for item in primary_list]
        primary_sql_str = ','.join(primary_sql_asc_list)
        if page == 1:
            logging.info('first page, default ""')
            self.cur_primary_value = ["'0'"]*len(primary_list)
        primary_value_filter_sql = self.__assemble_primary_key_and_value_sql(
            primary_list)

        sql = f'select * from (select {columns} from {owner}.{table_name} where {primary_value_filter_sql} order by {primary_sql_str}) table_alise where rownum <={self.default_per_page}'
        return sql

    def __assemble_primary_key_and_value_sql(self, primary_list: list):
        logging.info(f'{primary_list, self.cur_primary_value}')
        """ assemble primary key value """

        if len(primary_list) != len(self.cur_primary_value):
            raise Exception("len primary_list and value is not equle")

        primary_key_floor = []
        for index, item in enumerate(primary_list, start=0):
            cur_primary_key_floor = [
                f'({item} > {self.cur_primary_value[index]}']
            for front_index in range(0, index):
                cur_primary_key_floor.append(
                    f' AND {primary_list[front_index]} = {self.cur_primary_value[front_index]}')
            cur_primary_key_floor.append(')')
            cur_primary_sql = ''.join(cur_primary_key_floor)
            primary_key_floor.append(cur_primary_sql)
        primary_key_floor_sql = ' OR '.join(primary_key_floor)
        logging.info(primary_key_floor_sql)
        return primary_key_floor_sql

    def __update_cur_primary_value(self, source: list, dest: list, primary_key_index: dict):
        """ get little primary value """
        logging.info(f'{source, dest, primary_key_index}')
        if len(source) and len(dest):
            for index, key in enumerate(primary_key_index, start=0):
                primary_index = primary_key_index[key]
                source_data = source[primary_index]
                dest_data = dest[primary_index]
                little_primary_value = source_data if source_data > dest_data else dest_data
                self.cur_primary_value[index] = f"'{little_primary_value}'"
        elif not len(source) and len(dest):
            for index, key in enumerate(primary_key_index, start=0):
                primary_index = primary_key_index[key]
                self.cur_primary_value[index] = f"'{dest[primary_index]}'"
        elif len(source) and not len(dest):
            for index, key in enumerate(primary_key_index, start=0):
                primary_index = primary_key_index[key]
                self.cur_primary_value[index] = f"'{source[primary_index]}'"
        else:
            raise Exception(f'valid source {source} and dest {dest}')
        logging.info(f'cur primary value is {self.cur_primary_value}')

    def __get_oracle_table_last_row(self, table_data: list):
        """ get last row table data """
        result = []
        if len(table_data):
            result = table_data[-1]
        return result

    def __get_primary_key_index(self, primary_list: list, columns: list):
        """ get primary key index for columns """
        logging.info(primary_list)
        logging.info(columns)
        result = {}
        for item in primary_list:
            if item in columns:
                result[item] = columns.index(item)
        return result

    def __source_dest_last_row_compare(self, table_data: list, source: list, dest: list):
        """ compare source and dest last row data """
        logging.info('start compare last row...')
        columns = table_data[2].split(',')
        primarykey_list = table_data[-2].split(',')
        primary_key_index = self.__get_primary_key_index(
            primarykey_list, columns)
        source_last_data = self.__get_oracle_table_last_row(source)
        dest_last_data = self.__get_oracle_table_last_row(dest)

        self.__update_cur_primary_value(
            source_last_data, dest_last_data, primary_key_index)

    def __source_dest_data_compare(self, table_data:list, source, dest):
        """ compare source, dest data """
        logging.info('start compare source data and dest data')
        pass

    def __query_oracle_table_order_by_primary(self, table_data: list,  page: int):
        """ query oracle table order by primary """
        query_sql = self.__format_table_query_sql(table_data, page)
        logging.info(f'{query_sql}')

        source_data = self.source_oracle_db.get_oracle_table_by_sql(query_sql)
        dest_data = self.dest_oracle_db.get_oracle_table_by_sql(query_sql)
        logging.info(source_data)
        self.__source_dest_last_row_compare(table_data, source_data, dest_data)
        self.__source_dest_data_compare(table_data, source_data, dest_data)
