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
        self.default_per_page = 20
        self.last_page_count = 0
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
        self.__analysis_target_table_data(tables[15])

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
        table_name = table_data[1]
        columns = table_data[2]
        primarys = table_data[-2]
        primary_sql_asc_list = [f'{item} ASC' for item in primarys.split(',')]
        primary_sql_str = ','.join(primary_sql_asc_list)
        row_num_start = (page - 1) * self.default_per_page + 1
        row_num_end = page * self.default_per_page
        sql = f'select {columns} from (SELECT tt.*, ROWNUM as rowno from (SELECT t.* from {table_name} t) tt where ROWNUM <= {row_num_end} ORDER BY {primary_sql_str} ) table_alise where table_alise.ROWNO >={row_num_start}'
        logging.info(f'{sql}')

    def __query_oracle_table_order_by_primary(self, table_data: list,  page: int):
        """ query oracle table order by primary """
        table_name = table_data[1]
        logging.info(f'========== {self.default_per_page, page, self.last_page_count}')
        query_sql = self.__format_table_query_sql(table_data, page)
