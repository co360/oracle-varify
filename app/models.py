import sqlite3
import os
import logging


class SqliteDB:
    def __init__(self):
        self.__init_db()

    def __init_db(self):
        cur_dir = os.path.dirname(os.path.abspath('__file__'))
        model_path = 'app/oracle.db'
        db_path = os.path.join(cur_dir, model_path)
        logging.info(f'model path is {db_path}')

        try:
            self.db = sqlite3.connect(db_path)
            self.cursor = self.db.cursor()
        except expression as identifier:
            logging.error(f'connect sqlite error {identifier}')
            return False

    def check_table_exist(self, table):
        cursot_data = self.cursor.execute(
            f"select count(*) from sqlite_master where type='table' and name='{table}'")
        count = [item[0] for item in cursot_data][0]

        return True if count else False

    def create_table_table(self):
        oracle_table = 'oracle_tables'
        status = self.check_table_exist(oracle_table)
        if status:
            self.cursor.execute(f'drop table {oracle_table}')
        self.cursor.execute(f'''
            CREATE TABLE {oracle_table}(
                id INT PRIMARY KEY NOT NULL,           
                name CHAR(100) NOT NULL,          
                owner CHAR(100) NOT NULL,            
                status CHAR(20) NOT NULL,            
                num_rows CHAR(100) NOT NULL)'''
                            )
        self.db.commit()

    def close_connect(self):
        self.cursor.close()
        self.db.close()
