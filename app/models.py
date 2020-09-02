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

    def __sqlite_drop_table(self, cursor, table):
        """ drop target table from sqlite """
        check_table = f"select count(*) from sqlite_master where type='table' and name='{table}'"
        table_status = cursor.execute(check_table)
        if table_status:
            logging.warn(f'table {table} is exist, drop it')
            cursor.execute(f'drop table {table}')

    def oracle_drop_tables(self):
        """ drop all sqlite tables """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            self.__sqlite_drop_table(cursor, self.oracle_table)

    def __sqlite_oracle_table_create(self, cursor):
        """ create target table from sqlite """
        cursor.execute(f'''
                CREATE TABLE {self.oracle_table}(
                    p_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    name CHAR(100) NOT NULL,      
                    owner CHAR(100) NOT NULL,
                    tag CHAR(10) NOT NULL,
                    status CHAR(20) NOT NULL,
                    num_rows CHAR(100) NOT NULL)'''
                       )

    def oracle_tables_create(self):
        """ create all sqlite tables """
        with sqlite3.connect(self.db) as connection:
            cursor = connection.cursor()
            self.__sqlite_oracle_table_create(cursor)

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
                sql = f'INSERT INTO oracle_tables VALUES (NULL,"{name}", "{owner}", "{tag}", "{status}", "{num_rows}")'
                logging.info(f'insert into table sql is {sql}')
                cursor.execute(sql)
