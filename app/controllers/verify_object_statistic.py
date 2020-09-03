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
        """ 
        restruct list to object 
        data: [('name', 'owner', 'status')]
        return: {'OE': {'name', 'owner', 'statu'}}
        """
        logging.info(f'{data}')
    
    def __compare_object_data(self, object_name):
        """
        get source and dest data from sqlite
        object_name: table
        write source data to self._source_data
        write dest data to self._dest_data
        return Null
        """
        _source_data = self.__sqlite_db.sqlite_query_common_object_table(object_name, 'source')
        _dest_data = self.__sqlite_db.sqlite_query_common_object_table(object_name, 'dest')
        _source_data = self.__restruct_sqlite_data(_source_data)
        _dest_data = self.__restruct_sqlite_data(_dest_data)

    def __compare_init(self):
        """
        compare object data
        return Null
        """
        self.__compare_object_data('function')
        # self.__compare_object_data('index')

