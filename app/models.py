import sqlite3
import os
import logging
from .common.common import get_all_data_from_ini_file


class SqliteDB:
    def __init__(self):
        self.__config_init()

    def __config_init(self):
        model_path = os.path.join('app', 'oracle.db')
        ini_path = os.path.join('app', 'models.ini')
        ini_container = 'oracle_table_name'
        cur_dir = os.path.dirname(os.path.abspath('__file__'))
        self.db = os.path.join(cur_dir, model_path)
        oracle_table_name = get_all_data_from_ini_file(ini_path, ini_container)

        self.oracle_table = oracle_table_name['oracle_table']
        self.oracle_view = oracle_table_name['oracle_view']
        self.oracle_job = oracle_table_name['oracle_job']
        self.oracle_synonym = oracle_table_name['oracle_synonym']
        self.oracle_materialized_view = oracle_table_name['oracle_materialized_view']
        self.oracle_trigger = oracle_table_name['oracle_trigger']
        self.oracle_dblink = oracle_table_name['oracle_dblink']
        self.oracle_function = oracle_table_name['oracle_function']
        self.oracle_procedure = oracle_table_name['oracle_procedure']
        self.oracle_index = oracle_table_name['oracle_index']
        self.oracle_table_partition = oracle_table_name['oracle_table_partition']
        self.oracle_package = oracle_table_name['oracle_package']
        self.oracle_sequence = oracle_table_name['oracle_sequence']
        self.oracle_type = oracle_table_name['oracle_type']
        self.oracle_env_info = oracle_table_name['oracle_env_info']
        self.oracle_verify_object_statistic = oracle_table_name['oracle_verify_object_statistic']
        self.oracle_verify_each_object_data = oracle_table_name['oracle_verify_each_object_data']
        self.oracle_verify_table_row = oracle_table_name['oracle_verify_table_row']

    def __sqlite_drop_table(self, cursor, table):
        """ drop target table from sqlite """
        check_table = f"select count(*) from sqlite_master where type='table' and name='{table}'"
        logging.info(f'check table sql is {check_table}')
        table_status = [item[0] for item in cursor.execute(check_table)][0]
        logging.info(f'status is {table_status}')
        if table_status:
            logging.warn(f'table {table} is exist, drop it')
            cursor.execute(f'drop table {table}')

    def oracle_tables_drop(self):
        """ drop all sqlite tables """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            self.__sqlite_drop_table(cursor, self.oracle_table)
            self.__sqlite_drop_table(cursor, self.oracle_view)
            self.__sqlite_drop_table(cursor, self.oracle_job)
            self.__sqlite_drop_table(cursor, self.oracle_synonym)
            self.__sqlite_drop_table(cursor, self.oracle_materialized_view)
            self.__sqlite_drop_table(cursor, self.oracle_trigger)
            self.__sqlite_drop_table(cursor, self.oracle_dblink)
            self.__sqlite_drop_table(cursor, self.oracle_function)
            self.__sqlite_drop_table(cursor, self.oracle_procedure)
            self.__sqlite_drop_table(cursor, self.oracle_index)
            self.__sqlite_drop_table(cursor, self.oracle_table_partition)
            self.__sqlite_drop_table(cursor, self.oracle_package)
            self.__sqlite_drop_table(cursor, self.oracle_sequence)
            self.__sqlite_drop_table(cursor, self.oracle_type)
            self.__sqlite_drop_table(cursor, self.oracle_env_info)
            self.__sqlite_drop_table(
                cursor, self.oracle_verify_object_statistic)
            self.__sqlite_drop_table(
                cursor, self.oracle_verify_each_object_data)
            self.__sqlite_drop_table(cursor, self.oracle_verify_table_row)

    def __sqlite_oracle_table_create(self, cursor):
        """ create target table to sqlite """
        cursor.execute(f'''
                CREATE TABLE {self.oracle_table}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    name CHAR(100) NOT NULL,      
                    owner CHAR(100) NOT NULL,
                    tag CHAR(10) NOT NULL,
                    status CHAR(20) NOT NULL,
                    num_rows CHAR(100) NOT NULL)'''
                       )

    def __sqlite_oracle_common_table_create(self, cursor, table_name):
        """ create common ddl table to sqlite """
        cursor.execute(f'''
                CREATE TABLE {table_name}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    name CHAR(100) NOT NULL,      
                    owner CHAR(100) NOT NULL,
                    tag CHAR(10) NOT NULL,
                    status CHAR(20) NOT NULL)'''
                       )

    def __sqlite_oracle_env_info_create(self, cursor):
        """ create oracle statistic to sqlite """
        cursor.execute(f'''
                CREATE TABLE {self.oracle_env_info}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    name CHAR(100) NOT NULL,      
                    value CHAR(100) NOT NULL,
                    tag CHAR(10) NOT NULL)'''
                       )

    def __sqlite_oracle_verify_object_statistic_create(self, cursor):
        """ create oracle statistic to sqlite """
        cursor.execute(f'''
                CREATE TABLE {self.oracle_verify_object_statistic}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner CHAR(100) NOT NULL,      
                    object_name CHAR(100) NOT NULL,      
                    count_source INTEGER NOT NULL,
                    count_dest INTEGER NOT NULL,
                    count_error INTEGER NOT NULL)'''
                       )

    def __sqlite_oracle_verify_each_object_table_create(self, cursor):
        """ create oracle each object data to sqlite """
        cursor.execute(f'''
                CREATE TABLE {self.oracle_verify_each_object_data}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner CHAR(100) NOT NULL,      
                    name_source CHAR(100) NOT NULL,
                    name_dest CHAR(100) NOT NULL,
                    object_type CHAR(50) NOT NULL,
                    verify_status CHAR(10) NOT NULL)'''
                       )

    def __sqlite_oracle_verify_table_row_create(self, cursor):
        """ create oracle table row to sqlite """
        cursor.execute(f'''
                CREATE TABLE {self.oracle_verify_table_row}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner CHAR(100) NOT NULL,      
                    table_name CHAR(100) NOT NULL,
                    primary_group CHAR(100) NOT NULL,
                    primary_group_value CHAR(100) NOT NULL,
                    tag CHAR(10) NOT NULL)'''
                       )

    def __get_table_name(self, table_name):
        """ get define table name """
        result = ''
        if table_name == 'table':
            result = self.oracle_table
        elif table_name == 'view':
            result = self.oracle_view
        elif table_name == 'job':
            result = self.oracle_job
        elif table_name == 'synonym':
            result = self.oracle_synonym
        elif table_name == 'materialized_view':
            result = self.oracle_materialized_view
        elif table_name == 'trigger':
            result = self.oracle_trigger
        elif table_name == 'dblink':
            result = self.oracle_dblink
        elif table_name == 'function':
            result = self.oracle_function
        elif table_name == 'procedure':
            result = self.oracle_procedure
        elif table_name == 'index':
            result = self.oracle_index
        elif table_name == 'table_partition':
            result = self.oracle_table_partition
        elif table_name == 'package':
            result = self.oracle_package
        elif table_name == 'sequence':
            result = self.oracle_sequence
        elif table_name == 'type':
            result = self.oracle_type
        elif table_name == 'env_info':
            result = self.oracle_env_info

        if not result:
            logging.error(f'Target table name {table_name} is not exist')
            return False
        return result

    def sqlite_oracle_common_table_insert(self, data, table_name, tag):
        """ insert common ddl table to sqlite """
        if not data:
            logging.warn(f'Target table {table_name} is empty')
            return False
        table_name = self.__get_table_name(table_name)
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            for row_line in data:
                name = row_line[1]
                owner = row_line[0]
                tag = tag
                status = row_line[2]
                sql = f'INSERT INTO {table_name} VALUES (NULL,"{name}", "{owner}", "{tag}", "{status}")'
                logging.info(f'insert into table sql is {sql}')
                cursor.execute(sql)

    def sqlite_oracle_env_info_insert(self, data, tag):
        """ statistic oracle data """
        if not data:
            logging.warn(f'Target table {self.oracle_env_info} is empty')
            return False
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            for key, value in data.items():
                sql = f'INSERT INTO {self.oracle_env_info} VALUES (NULL, "{key}", "{value}", "{tag}")'
                logging.info(f'insert into table sql is {sql}')
                cursor.execute(sql)

    def oracle_tables_create(self):
        """ create all sqlite tables """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            self.__sqlite_oracle_table_create(cursor)
            self.__sqlite_oracle_common_table_create(cursor, self.oracle_view)
            self.__sqlite_oracle_common_table_create(cursor, self.oracle_job)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_synonym)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_materialized_view)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_trigger)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_dblink)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_function)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_procedure)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_index)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_table_partition)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_package)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_sequence)
            self.__sqlite_oracle_common_table_create(
                cursor, self.oracle_type)
            self.__sqlite_oracle_env_info_create(cursor)
            self.__sqlite_oracle_verify_object_statistic_create(cursor)
            self.__sqlite_oracle_verify_each_object_table_create(cursor)
            self.__sqlite_oracle_verify_table_row_create(cursor)

    def sqlite_table_insert(self, data, tag):
        """ create oracle table """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            for row_line in data:
                name = row_line[1]
                owner = row_line[0]
                tag = tag
                status = row_line[2]
                num_rows = row_line[3]
                sql = f'INSERT INTO {self.oracle_table} VALUES (NULL,"{name}", "{owner}", "{tag}", "{status}", "{num_rows}")'
                logging.info(f'insert into table sql is {sql}')
                cursor.execute(sql)
    
    def sqlite_common_object_table_query(self, table_name, tag, owner):
        """ return object table data """
        table_name = self.__get_table_name(table_name)
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            sql = f'select name, owner, status from {table_name} where tag = "{tag}" and owner = "{owner}" ORDER BY name ASC'
            result = cursor.execute(sql)
            return list(result)
    
    def sqlite_table_object_query(self, tag, owner):
        """ return table objects data """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            sql = f'select name, owner, status, num_rows from {self.oracle_table} where tag = "{tag}" and owner = "{owner}" ORDER BY name ASC'
            result = cursor.execute(sql)
            return list(result)

    def sqlite_env_info_object_query(self, tag):
        """ return oracle env info objects data """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            sql = f'select name, value from {self.oracle_env_info} where tag = "{tag}" ORDER BY name ASC'
            result = cursor.execute(sql)
            return list(result)

    def sqlite_verify_object_statistic_insert(self, data):
        """ insert object table data """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            object_name = data['object_name']
            count_source = data['count_source']
            count_dest = data['count_dest']
            count_error = data['count_error']
            owner = data['owner']
            sql = f'INSERT INTO {self.oracle_verify_object_statistic} VALUES (NULL, "{owner}", "{object_name}", "{count_source}", "{count_dest}", "{count_error}")'
            cursor.execute(sql)

    def sqlite_verify_each_object_insert(self, data):
        """ insert object table data """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            owner = data['owner']
            object_type = data['object_type']
            name_source = data['name_source']
            name_dest = data['name_dest']
            verify_status = data['verify_status']
            sql = f'INSERT INTO {self.oracle_verify_each_object_data} VALUES (NULL, "{owner}", "{name_source}", "{name_dest}", "{object_type}", "{verify_status}")'
            cursor.execute(sql)

    def sqlite_verify_object_statistic_query(self):
        """ return oracle objects statics data """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            sql = f'select owner, object_name, count_source, count_dest, count_error from {self.oracle_verify_object_statistic} ORDER BY owner ASC'
            result = cursor.execute(sql)
            return list(result)

    def sqlite_oracle_verify_each_object_table_query(self, object_name):
        """ return verify each object table data """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            sql = f'select owner, name_source, name_dest, verify_status from {self.oracle_verify_each_object_data} where object_type = "{object_name}" ORDER BY owner ASC'
            result = cursor.execute(sql)
            return list(result)