import logging
import os
from copy import deepcopy
from ..common.common import varify_data_use_md5
from ..models import SqliteDB


class VerifyObjectStatistic:
    """
    verify every object 
    """

    def __init__(self, users):
        self.sqlite_db = SqliteDB()
        self.__compare_init(users)

    def __count_more_objects(self, object_data, object_type, user):
        keys = object_data.keys()
        result = 0

        for object_name, object_item in object_data.items():
            result += 1
            self.__insert_verify_each_object_table(
                user, '__lose__', object_name, object_type, False)
        return result

    def __compare_object_md5(self, source, dest, user, object_type):
        """ 
        compare object md5 verify
        """
        count = 0
        source_obj = {item[0]: item for item in source}
        dest_obj = {item[0]: item for item in dest}

        for source_item in source:
            source_object_name = source_item[0]
            if source_object_name in dest_obj:
                logging.info(f'source {source_item}')
                logging.info(f'dest {dest_obj[source_object_name]}')
                status = varify_data_use_md5(
                    source_item, dest_obj[source_object_name])
                dest_obj.pop(source_object_name)
                source_obj.pop(source_object_name)
                logging.info(f'varify is {status}')
                if not status:
                    count += 1
                self.__insert_verify_each_object_table(
                    user, source_object_name, source_object_name, object_type, status)
            else:
                self.__insert_verify_each_object_table(
                    user, source_object_name, '__lose__', object_type, False)
                count += 1

        dest_more_count = self.__count_more_objects(
            dest_obj, object_type, user)
        count += dest_more_count

        return count

    def __insert_verify_object_table(self, owner, object_name, count_source, count_dest, count_error):
        self.sqlite_db.sqlite_verify_object_statistic_insert({
            'object_name': object_name,
            'count_source': count_source,
            'count_dest': count_dest,
            'count_error': count_error,
            'owner': owner
        })

    def __insert_verify_each_object_table(self, owner, name_source, name_dest, object_type, verify_status):
        self.sqlite_db.sqlite_verify_each_object_insert({
            'owner': owner,
            'name_source': name_source,
            'name_dest': name_dest,
            'object_type': object_type,
            'verify_status': str(verify_status),
        })

    def __compare_object_data(self, object_name, user):
        """
        get source and dest data from sqlite
        """
        source_data = self.sqlite_db.sqlite_common_object_table_query(
            object_name, 'source', user)
        dest_data = self.sqlite_db.sqlite_common_object_table_query(
            object_name, 'dest', user)
        source_count = len(source_data)
        dest_count = len(dest_data)
        verify_count = self.__compare_object_md5(
            source_data, dest_data, user, object_name)
        logging.info(f'{source_count}, {dest_count}, {verify_count}')
        self.__insert_verify_object_table(
            user, object_name, source_count, dest_count, verify_count)

    def __compare_init(self, users):
        """
        compare object data
        """
        for user in users:
            self.__compare_object_data('view', user)
            self.__compare_object_data('job', user)
            self.__compare_object_data('synonym', user)
            self.__compare_object_data('materialized_view', user)
            self.__compare_object_data('trigger', user)
            self.__compare_object_data('dblink', user)
            self.__compare_object_data('function', user)
            self.__compare_object_data('procedure', user)
            self.__compare_object_data('index', user)
            self.__compare_object_data('table_partition', user)
            self.__compare_object_data('package', user)
            self.__compare_object_data('sequence', user)
            self.__compare_object_data('type', user)
            self.__compare_object_data('table', user)
