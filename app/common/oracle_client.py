#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 wanghao <wanghao054@chinasoftinc.com>
#
# Distributed under terms of the MIT license.

"""
connect oracle database
"""
import cx_Oracle
import json
import logging
import time
import os
from .common import get_all_data_from_ini_file


class OracleDB:
    def __init__(self, data):
        self.data = data
        self.db = None
        self.__config_init()

    def __config_init(self):
        """ init data from ini file """
        ini_path = os.path.join('app', 'common', 'oracle_sql.ini')
        ini_container = 'oracle_sql'
        self.sql_data = get_all_data_from_ini_file(ini_path, ini_container)

    def connect_oracle(self):
        logging.info(self.data['user'])
        try:
            self.db = cx_Oracle.connect(
                self.data['user'],
                self.data['password'],
                f"{self.data['host']}:{self.data['port']}/{self.data['sid']}",
                mode=cx_Oracle.DEFAULT_AUTH,
                encoding="UTF-8")
        except:
            logging.error('userInfo error {}'.format(str(self.data)))
            return False
        else:
            return True

    def env_init(self):
        while not self.db:
            time.sleep(1)
            self.connect_oracle()
        self.cursor = self.db.cursor()

    def __get_user_table_status(self, user):
        sql = self.sql_data['get_user_table_status']
        sql = sql.format(owner=user)
        logging.info(f'====== table sql is {sql}')
        return {item[1]: list(item) for item in self.cursor.execute(sql)}

    def __get_user_views(self, user):
        """ get user views """
        sql = self.sql_data['get_user_view_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_tables(self, user):
        """ get user tables """
        sql = self.sql_data['get_user_table_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_jobs(self, user):
        """ get user jobs """
        sql = self.sql_data['get_user_job_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_types(self, user):
        """ get user type """
        sql = self.sql_data['get_user_type_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_sequences(self, user):
        """ get user sequences """
        sql = self.sql_data['get_user_sequence_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_packages(self, user):
        """ get user packages """
        sql = self.sql_data['get_user_package']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_table_pratitions(self, user):
        """ get user table_partitions """
        sql = self.sql_data['get_user_table_partition_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_indexs(self, user):
        """ get user indexs """
        sql = self.sql_data['get_user_index_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_procedures(self, user):
        """ get user procedures """
        sql = self.sql_data['get_user_procedure_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_functions(self, user):
        """ get user functions """
        sql = self.sql_data['get_user_function_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_dblinks(self, user):
        """ get user dblinks """
        sql = self.sql_data['get_user_dblink']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_triggers(self, user):
        """ get user triggers """
        sql = self.sql_data['get_user_triggers']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_materialized_views(self, user):
        """ get user materialized_view """
        sql = self.sql_data['get_user_materialized_view']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def __get_user_synonyms(self, user):
        """ get user synonyms """
        sql = self.sql_data['get_user_synonym_status']
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def get_oracle_version(self):
        """ get oracle version """
        sql = self.sql_data['get_oracle_version']
        oracle_version = [item[0] for item in self.cursor.execute(sql)][0]
        return oracle_version

    def get_oracle_charcode(self):
        """ get oracle char code """
        sql = self.sql_data['get_oracle_charcode']
        charcode = [item[0] for item in self.cursor.execute(sql)]
        result = '-'.join(charcode)
        return result

    def get_oracle_page_size(self):
        """ get oracle page size """
        sql = self.sql_data['get_oracle_page_size']
        pagesize = [item for item in self.cursor.execute(sql)][0]
        result = '-'.join(pagesize)
        return result

    def get_oracle_nls_comp(self):
        """ get oracle nls comp """
        sql = self.sql_data['get_oracle_nls_comp']
        nls_comp = [item for item in self.cursor.execute(sql)][0]
        nls_comp = [str(item) for item in nls_comp]
        result = '-'.join(nls_comp)
        return result

    def get_oracle_nls_sort(self):
        """ get oracle nls sort """
        sql = self.sql_data['get_oracle_nls_sort']
        nls_sort = [item for item in self.cursor.execute(sql)][0]
        nls_sort = [str(item) for item in nls_sort]
        result = '-'.join(nls_sort)
        return result

    def get_user_objects(self, user, object_name):
        """ get common result objects """
        result = None
        if object_name == 'view':
            result = self.__get_user_views(user)
        elif object_name == 'table':
            result = self.__get_user_tables(user)
        elif object_name == 'job':
            result = self.__get_user_jobs(user)
        elif object_name == 'synonym':
            result = self.__get_user_synonyms(user)
        elif object_name == 'materialized_view':
            result = self.__get_user_materialized_views(user)
        elif object_name == 'trigger':
            result = self.__get_user_triggers(user)
        elif object_name == 'dblink':
            result = self.__get_user_dblinks(user)
        elif object_name == 'function':
            result = self.__get_user_functions(user)
        elif object_name == 'procedure':
            result = self.__get_user_procedures(user)
        elif object_name == 'index':
            result = self.__get_user_indexs(user)
        elif object_name == 'table_partition':
            result = self.__get_user_table_pratitions(user)
        elif object_name == 'package':
            result = self.__get_user_packages(user)
        elif object_name == 'sequence':
            result = self.__get_user_sequences(user)
        elif object_name == 'type':
            result = self.__get_user_types(user)
        else:
            logging.error(f'Target object {object_name} is error')
            return False
        return result

    def close(self):
        """ close connect oracle """
        self.db.close()
