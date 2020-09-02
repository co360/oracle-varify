#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2020 wanghao <wanghao054@chinasoftinc.com>
#
# Distributed under terms of the MIT license.

"""

"""
import os
import logging
from configparser import ConfigParser
from ..common.oracle_client import OracleDB
from ..models import SqliteDB


def get_data_from_oracle_config(type):
    cur_dir = os.path.dirname(os.path.abspath('__file__'))
    config_path = 'config.ini'
    config_abs_path = os.path.join(cur_dir, config_path)
    logging.info(f'config path is {config_abs_path}')

    config = ConfigParser()
    config.read(config_abs_path)

    if type in config:
        result = config[type]
        return dict(result)
    else:
        logging.error(f'{type} is not exist in config.ini')
        return False


def collect_oracle_tables(oracle_db, sqlite_db, user, tag):
    """ collect oracle tables data """
    user_tables = oracle_db.get_user_tables_data(user)
    sqlite_db.sqlite_table_insert(user_tables, tag)
    logging.info(f'{user} table is {user_tables}')


def collect_oracle_common_object(oracle_db, sqlite_db, object_name, user, tag):
    """ collect common objects data """
    user_objects = oracle_db.get_user_objects(user, object_name)
    logging.info(f'====== {user_objects}')
    sqlite_db.sqlite_oracle_common_table_insert(user_objects, object_name, tag)


def collect_oracle_data(sqlite_db, config, users, tag):
    """ collect oracle data and insert data to sqlite """
    oracle_db = OracleDB(config)
    status = oracle_db.connect_oracle()
    logging.info(f'status {status}')

    if not status:
        logging.error(f'Connect oracle error, {config}')
        return False

    oracle_db.env_init()

    for user in users:
        # collect_oracle_tables(oracle_db, sqlite_db, user, tag)
        # collect_oracle_common_object(oracle_db, sqlite_db, 'view', user, tag)
        # collect_oracle_common_object(oracle_db, sqlite_db, 'job', user, tag)
        # collect_oracle_common_object(oracle_db, sqlite_db, 'synonym', user, tag)
        # collect_oracle_common_object(oracle_db, sqlite_db, 'materialized_view', user, tag)
        # collect_oracle_common_object(
        # oracle_db, sqlite_db, 'trigger', user, tag)
        # collect_oracle_common_object(
        # oracle_db, sqlite_db, 'dblink', user, tag)
        # collect_oracle_common_object(
        #     oracle_db, sqlite_db, 'function', user, tag)
        # collect_oracle_common_object(
        #     oracle_db, sqlite_db, 'procedure', user, tag)
        # collect_oracle_common_object(
        #     oracle_db, sqlite_db, 'index', user, tag)
        # collect_oracle_common_object(
        #     oracle_db, sqlite_db, 'table_partition', user, tag)
        # collect_oracle_common_object(
        #     oracle_db, sqlite_db, 'package', user, tag)
        # collect_oracle_common_object(
        #     oracle_db, sqlite_db, 'sequence', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'type', user, tag)


def sqlite_db_reset(sqlite_db):
    """ delete sqlite all tables """
    sqlite_db.oracle_tables_drop()
    sqlite_db.oracle_tables_create()


def collect_oracle_init():
    """ start to collect oracle init """
    source_oracle_config = get_data_from_oracle_config('source')
    dest_oracle_config = get_data_from_oracle_config('dest')
    dvt_config = get_data_from_oracle_config('dvt')
    target_users = dvt_config['verify_schema']

    if not target_users:
        logging.error(
            'verify_schema is empty, please input verify_schema for config.ini')
        return False

    users = target_users.split(',')

    sqlite_db = SqliteDB()
    sqlite_db_reset(sqlite_db)

    collect_oracle_data(sqlite_db, source_oracle_config, users, 'source')
    collect_oracle_data(sqlite_db, dest_oracle_config, users, 'dest')
