import logging
import os
from copy import deepcopy
from ..common.common import varify_data_use_md5
from ..models import SqliteDB


class VerifyObjectStatistic:
    """
    verify every object 
    """

    def __init__(self):
        self.__sqlite_db = SqliteDB()
        self.__compare_init()
        self.__source_data = {}
        self.__dest_data = {}

    def __restruct_sqlite_data(self, data):
        logging.info(f'{data}')

    def __compare_init(self):
        _source_data = self.__sqlite_db.sqlite_query_common_object_table('function', 'source')
        _source_data = self.__restruct_sqlite_data(_source_data)

