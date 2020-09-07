import os
import math
import logging
import datetime
from ..common.oracle_client import OracleDB
from ..models import SqliteDB
from ..common.common import get_data_from_oracle_config_ini
from ..common.common import varify_data_use_md5


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
        self.primary_value_floor = ''
        self.primary_value_ceil = ''
        self.primary_key_index = {'primary_key': 1}
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
        owner = table_data[0]
        columns = table_data[2]
        max_rows = table_data[3]
        primary_keys = table_data[4]
        primary_types = table_data[-2]
        varify_percent = table_data[-1]
        logging.info(table_data)
        max_rows = self.__get_table_max_rows(table_name, max_rows)
        varify_rows = self.__get_table_verify_rows(
            table_name, max_rows, varify_percent)
        pages = self.__get_table_per_pages(table_name, varify_rows)
        self.__get_primary_key_index(columns, primary_keys)

        for page in range(1, pages + 1):
            self.__query_oracle_table_order_by_primary(
                owner, table_name, columns, page)
        self.__query_oracle_table_order_by_primary(
            owner, table_name, columns, pages+1)

    def __format_table_query_sql(self, owner: str, table_name: str, columns: str,  page: int):
        """ format table query sql """
        primary_keys = self.primary_key_index.keys()
        primary_sql_asc_list = [f'{item} ASC' for item in primary_keys]
        primary_sql_str = ','.join(primary_sql_asc_list)
        if page == 1:
            logging.info('first page, default ""')
            self.primary_value_floor = ["'0'"]*len(primary_keys)
            self.primary_value_ceil = ["'0'"]*len(primary_keys)
        primary_value_filter_sql = self.__assemble_primary_key_and_value_sql()

        sql = f'select * from (select {columns} from {owner}.{table_name} where {primary_value_filter_sql} order by {primary_sql_str}) table_alise where rownum <={self.default_per_page}'
        return sql

    def __assemble_primary_key_and_value_sql(self):
        """ 
        assemble primary key value 
        every query use last primary value ceil as cur primary floor
        """
        primary_keys = self.primary_key_index.keys()
        logging.info(f'{primary_keys, self.primary_value_ceil}')

        if len(primary_keys) != len(self.primary_value_ceil):
            raise Exception("len primary_list and value is not equle")

        primary_key_floor = []
        for index, item in enumerate(primary_keys, start=0):
            cur_primary_key_floor = [
                f'({item} > {self.primary_value_ceil[index]}']
            for front_index in range(0, index):
                cur_primary_key_floor.append(
                    f' AND {primary_keys[front_index]} = {self.primary_value_ceil[front_index]}')
            cur_primary_key_floor.append(')')
            cur_primary_sql = ''.join(cur_primary_key_floor)
            primary_key_floor.append(cur_primary_sql)
        primary_key_floor_sql = ' OR '.join(primary_key_floor)
        logging.info(primary_key_floor_sql)
        return primary_key_floor_sql

    def __update_primary_value_ceil_or_floor(self, source: list, dest: list,  type: str):
        """ get little primary value """
        primary_key_index = self.primary_key_index
        logging.info(f'{source, dest, primary_key_index}')
        if type == 'ceil':
            primary_value = self.primary_value_ceil
        elif type == 'floor':
            primary_value = self.primary_value_floor

        if len(source) and len(dest):
            for index, key in enumerate(primary_key_index, start=0):
                primary_index = primary_key_index[key]
                source_data = source[primary_index]
                dest_data = dest[primary_index]
                little_primary_value = source_data if source_data < dest_data else dest_data
                primary_value[index] = f"'{little_primary_value}'"
        elif not len(source) and len(dest):
            for index, key in enumerate(primary_key_index, start=0):
                primary_index = primary_key_index[key]
                primary_value[index] = f"'{dest[primary_index]}'"
        elif len(source) and not len(dest):
            for index, key in enumerate(primary_key_index, start=0):
                primary_index = primary_key_index[key]
                primary_value[index] = f"'{source[primary_index]}'"
        else:
            raise Exception(f'valid source {source} and dest {dest}')

        if type == 'ceil':
            self.primary_value_ceil = primary_value
        elif type == 'floor':
            self.primary_key_floor = primary_value

        logging.info(
            f'cur primary value {type} {self.primary_value_floor, self.primary_value_ceil}')

    def __get_oracle_table_first_or_last_row(self, table_data: list, type: str):
        """ get first or last row table data """
        result = []
        if len(table_data):
            if type == 'last':
                result = table_data[-1]
            elif type == 'first':
                result = table_data[0]
        return result

    def __get_primary_key_index(self, columns: str, primary_keys: str):
        """ get primary key index for columns """
        logging.info(f'columns is {columns}')
        logging.info(f'primary key list is {primary_keys}')
        column_list = columns.split(',')
        primary_key_list = primary_keys.split(',')
        result = {}
        for item in primary_key_list:
            if item in column_list:
                result[item] = column_list.index(item)
        self.primary_key_index = result
        return result

    def __source_dest_last_row_compare(self, source: list, dest: list):
        """ compare source and dest last row data """
        logging.info('start compare last row...')
        source_last_data = self.__get_oracle_table_first_or_last_row(
            source, 'last')
        dest_last_data = self.__get_oracle_table_first_or_last_row(
            dest, 'last')
        source_first_data = self.__get_oracle_table_first_or_last_row(
            source, 'first')
        dest_first_data = self.__get_oracle_table_first_or_last_row(
            dest, 'first')

        self.__update_primary_value_ceil_or_floor(
            source_last_data, dest_last_data,  'ceil')
        self.__update_primary_value_ceil_or_floor(
            source_first_data, dest_last_data,  'floor')

    def __source_dest_data_compare(self, owner, table_name,  source: list, dest: list, page: int):
        """ compare source, dest data """
        logging.info('start compare source data and dest data')
        logging.info(f'source {source}, dest {dest}')
        status = varify_data_use_md5(source, dest)
        if status:
            logging.info(f'{table_name} compare ok')
        else:
            logging.error(f'{table_name} compare error')
        self.__update_table_foreach_query(owner, table_name, page, status)

    def __oracle_table_block_verify_ok_insert(self, table_data, source, page):
        """ oracle block table data compare equal """
        for index in range(0, self.default_per_page):
            result = {

            }

        for index in range(0, self.default_per_page):
            pass

    def __del_quota_mark_from_primary_value(self, primary_value: list):
        """
        delete quota mark from primary value 
        currend mysql query don't need
        """
        primary_str = ','.join(primary_value)
        result = primary_str.replace("'", '')
        return result

    def __update_table_foreach_query(self, owner: str, table_name: str, page: int, verify_status: bool):
        """ update foreach table every query info """

        primary_value_ceil = self.__del_quota_mark_from_primary_value(
            self.primary_value_ceil)
        primary_value_floor = self.__del_quota_mark_from_primary_value(
            self.primary_value_floor)
        result = {
            'owner': owner,
            'table_name': table_name,
            'page': page,
            'datetime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'per_page': self.default_per_page,
            'primary_value_ceil': primary_value_ceil,
            'primary_value_floor': primary_value_floor,
            'verify_status': str(verify_status)
        }
        logging.info(result)

        self.sqlite_db.sqlite_oracle_table_foreach_query_insert(result)

    def __query_oracle_table_order_by_primary(self, owner: str, table_name: str, columns: str,  page: int):
        """ query oracle table order by primary """
        query_sql = self.__format_table_query_sql(
            owner, table_name, columns, page)
        logging.info(f'{query_sql}')

        source_data = self.source_oracle_db.get_oracle_table_by_sql(query_sql)
        dest_data = self.dest_oracle_db.get_oracle_table_by_sql(query_sql)
        logging.info(source_data)
        self.__source_dest_last_row_compare(source_data, dest_data)
        self.__source_dest_data_compare(
            owner, table_name, source_data, dest_data, page)
