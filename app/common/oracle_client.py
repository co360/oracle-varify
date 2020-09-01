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
        logging.info(f'===== curdir {cur_dir}')
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

    def get_user_tables(self, user):
        sql = self.get_sql_from_ini('get_user_tables')
        sql = sql.format(OWNER=user)
        return [ item for item in self.cursor.execute(sql) ]

    def get_all_users(self):
        sql = self.get_sql_from_ini('get_all_users')
        return { item[0] for item in self.cursor.execute(sql) }

    def get_user_views(self, user):
        sql = self.get_sql_from_ini('getUserViews')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_view_text(self, user, view_name):
        sql = self.get_sql_from_ini('getViewText')
        sql = sql.format(owner=user, view_name=view_name)
        return [ item[0] for item in self.cursor.execute(sql) ][0]

    def get_user_functions(self, user):
        sql = self.get_sql_from_ini('getUserFunctions')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_function_text(self, user, function_name):
        sql = self.get_sql_from_ini('getFunctionText')
        sql = sql.format(owner=user, function_name=function_name)
        result = [ item[0] for item in self.cursor.execute(sql) ]
        return '\n'.join(result)

    def get_user_procedures(self, user):
        sql = self.get_sql_from_ini('getUserProcedures')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_procedure_text(self, user, procedure_name):
        sql = self.get_sql_from_ini('getProcedureText')
        sql = sql.format(owner=user, procedure_name=procedure_name)
        result = [ item[0] for item in self.cursor.execute(sql) ]
        return '\n'.join(result)

    def get_user_sequences(self, user):
        sql = self.get_sql_from_ini('getUserSequences')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_user_triggers(self, user):
        sql = self.get_sql_from_ini('getUserTriggers')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_user_packages(self, user):
        sql = self.get_sql_from_ini('getUserPackages')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_user_materialized_views(self, user):
        sql = self.get_sql_from_ini('getUserMaterializedViews')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_user_types(self, user):
        sql = self.get_sql_from_ini('getUserTypes')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_user_synonyms(self, user):
        sql = self.get_sql_from_ini('getUserSynonyms')
        sql = sql.format(owner=user)
        return { item[0] for item in self.cursor.execute(sql) }

    def get_oracle_version(self):
        sql = self.get_sql_from_ini('getOracleVersion')
        basicInfo = [ item for item in self.cursor.execute(sql) ][1]
        return '-'.join(basicInfo)

    def get_database_name(self):
        sql = self.get_sql_from_ini('getDatabaseName')
        database_name = [ item[0] for item in self.cursor.execute(sql)][0]
        return database_name

    def get_storage_type(self):
        sql = self.get_sql_from_ini('getStorageType')
        storage_type = [ item for item in self.cursor.execute(sql)][0]
        return '-'.join(storage_type)

    def get_charcode(self):
        sql = self.get_sql_from_ini('getCharCode')
        charcode = [ item for item in self.cursor.execute(sql)][0]
        return '-'.join(charcode)

    def get_table_size(self, table_name):
        sql = self.get_sql_from_ini('getTableSize')
        sql = sql.format(table=table_name)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if result:
            size = result[0][1]
        else:
            size = 0

        return size

    def close(self):
        self.db.close()
