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
from .verify_object_statistic import VerifyObjectStatistic
from .export_each_object_excel import ExportEachObjectExcel
from ..common.oracle_client import OracleDB
from ..common.common import get_data_from_oracle_config_ini
from ..models import SqliteDB


def collect_oracle_common_object(oracle_db: OracleDB, sqlite_db: SqliteDB, object_name, user, tag):
    """ collect common objects data """
    user_objects = oracle_db.get_user_objects(user, object_name)
    logging.info(f'====== {user_objects}')
    sqlite_db.sqlite_oracle_common_table_insert(user_objects, object_name, tag)


def collect_oracle_env_info_data(oracle_db: OracleDB, sqlite_db: SqliteDB, tag):
    """ collect oracle statistic data """
    oracle_version = oracle_db.get_oracle_version()
    oracle_charcode = oracle_db.get_oracle_charcode()
    oracle_nls_comp = oracle_db.get_oracle_nls_comp()
    oracle_nls_sort = oracle_db.get_oracle_nls_sort()
    oracle_page_size = oracle_db.get_oracle_page_size()
    sqlite_db.sqlite_oracle_env_info_insert({
        'oracle_version': oracle_version,
        'oracle_charcode': oracle_charcode,
        'oracle_nls_comp': oracle_nls_comp,
        'oracle_nls_sort': oracle_nls_sort,
        'oracle_page_size': oracle_page_size,
    }, tag)


def collect_oracle_data(sqlite_db: SqliteDB, config, users, tag):
    """ collect oracle data and insert data to sqlite """
    oracle_db = OracleDB(config)
    oracle_db.connect_oracle()
    oracle_db.env_init()
    collect_oracle_env_info_data(oracle_db, sqlite_db, tag)

    for user in users:
        collect_oracle_common_object(oracle_db, sqlite_db, 'table', user, tag)
        collect_oracle_common_object(oracle_db, sqlite_db, 'view', user, tag)
        collect_oracle_common_object(oracle_db, sqlite_db, 'job', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'synonym', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'materialized_view', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'trigger', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'dblink', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'function', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'procedure', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'index', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'table_partition', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'package', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'sequence', user, tag)
        collect_oracle_common_object(
            oracle_db, sqlite_db, 'type', user, tag)


def sqlite_db_reset(sqlite_db: SqliteDB):
    """ delete sqlite all tables """
    sqlite_db.oracle_tables_drop()
    sqlite_db.oracle_tables_create()


def collect_oracle_init():
    """ start to collect oracle init """
    source_oracle_config = get_data_from_oracle_config_ini('source')
    dest_oracle_config = get_data_from_oracle_config_ini('dest')
    dvt_config = get_data_from_oracle_config_ini('dvt')
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
    VerifyObjectStatistic(users)
    ExportEachObjectExcel()
