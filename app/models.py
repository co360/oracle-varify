import sqlite3
import os
import logging


class SqliteDB:
    def __init__(self):
        self.__config_init()

    def __config_init(self):
        model_path = 'app/oracle.db'
        cur_dir = os.path.dirname(os.path.abspath('__file__'))
        self.db = os.path.join(cur_dir, model_path)
        self.oracle_table = 'oracle_tables'
        self.oracle_view = 'oracle_views'
        self.oracle_job = 'oracle_jobs'
        self.oracle_synonym = 'oracle_synonyms'
        self.oracle_materialized_view = 'oracle_materialized_views'
        self.oracle_trigger = 'oracle_triggers'
        self.oracle_dblink = 'oracle_dblinks'
        self.oracle_function = 'oracle_functions'

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
