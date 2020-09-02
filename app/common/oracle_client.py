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
from configparser import ConfigParser
import os


class OracleDB:
    def __init__(self, data):
        self.data = data
        self.db = None
        logging.info(f'======= {data}')

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

    def get_sql_from_ini(self, key):
        config = ConfigParser()
        cur_dir = os.path.dirname(os.path.abspath('__file__'))
        config_path = os.path.join(cur_dir, 'app/common/oracle_sql.ini')
        if not os.path.exists(config_path):
            logging.error(f'{config_path} is not exist, please check')
            return False

        config.read(config_path)
        oracle_sql = config['oracle_sql']
        if key in oracle_sql:
            return oracle_sql.get(key)
        else:
            logging.error(f'Target key is not exist')
            return False

    def __get_user_table_status(self, user):
        sql = self.get_sql_from_ini('get_user_table_status')
        sql = sql.format(owner=user)
        logging.info(f'====== table sql is {sql}')
        return {item[1]: list(item) for item in self.cursor.execute(sql)}

    def __get_user_table_num_rows(self, user):
        sql = self.get_sql_from_ini('get_user_table_num_rows')
        sql = sql.format(owner=user)
        logging.info(f'====== table sql is {sql}')
        return {item[0]: str(item[1]) for item in self.cursor.execute(sql)}

    def get_user_tables_data(self, user):
        user_table_status = self.__get_user_table_status(user)
        user_table_num_rows = self.__get_user_table_num_rows(user)
        result = []

        for table_name, table_data in user_table_status.items():
            table_line = table_data
            if table_name in user_table_num_rows:
                table_data.append(user_table_num_rows[table_name])
            else:
                table_line.append('None')
            result.append(table_line)
        return result

    def get_user_views(self, user):
        """ get user views """
        sql = self.get_sql_from_ini('get_user_view_status')
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def get_user_jobs(self, user):
        """ get user jobs """
        sql = self.get_sql_from_ini('get_user_job_status')
        sql = sql.format(owner=user)
        result = [list(item) for item in self.cursor.execute(sql)]
        return result

    def get_user_functions(self, user):
        sql = self.get_sql_from_ini('get_user_functions')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_procedures(self, user):
        sql = self.get_sql_from_ini('get_user_procedures')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_sequences(self, user):
        sql = self.get_sql_from_ini('get_user_sequences')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_triggers(self, user):
        sql = self.get_sql_from_ini('get_user_triggers')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_packages(self, user):
        sql = self.get_sql_from_ini('get_user_packages')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_materialized_views(self, user):
        sql = self.get_sql_from_ini('get_user_materialized_views')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_types(self, user):
        sql = self.get_sql_from_ini('get_user_types')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_user_synonyms(self, user):
        sql = self.get_sql_from_ini('get_user_synonyms')
        sql = sql.format(owner=user)
        result = {item[0] for item in self.cursor.execute(sql)}
        return result

    def get_oracle_version(self):
        sql = self.get_sql_from_ini('get_oracle_version')
        basicInfo = [item for item in self.cursor.execute(sql)][1]
        result = '-'.join(basicInfo)
        return result

    def get_database_name(self):
        sql = self.get_sql_from_ini('get_database_name')
        database_name = [item[0] for item in self.cursor.execute(sql)][0]
        return database_name

    def get_storage_type(self):
        sql = self.get_sql_from_ini('get_storage_type')
        storage_type = [item for item in self.cursor.execute(sql)][0]
        result = '-'.join(storage_type)
        return result

    def get_charcode(self):
        sql = self.get_sql_from_ini('get_charcode')
        charcode = [item for item in self.cursor.execute(sql)][0]
        result = '-'.join(charcode)
        return result

    def get_table_size(self, table_name):
        sql = self.get_sql_from_ini('get_table_size')
        sql = sql.format(table=table_name)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if result:
            size = result[0][1]
        else:
            size = 0
        return size
    
    def get_user_objects(self, user, object_name):
        """ get common result objects """
        result = None
        if object_name == 'view':
            result = self.get_user_views(user)
        elif object_name == 'job':
            result = self.get_user_jobs(user)
        else:
            logging.error(f'Target object {object_name} is error')
            return False
        return result

    def close(self):
        """ close connect oracle """
        self.db.close()
