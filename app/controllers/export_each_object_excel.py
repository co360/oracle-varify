from openpyxl import Workbook
from openpyxl.styles import Alignment
import logging
import os
import datetime
import shutil

class ExportEachObjectExcel:
    def __init__(self):
        self.book = Workbook()
        self.__excel_file_path_config()
        self.__excel_save()
    
    def __excel_file_path_config(self):
        date_str = datetime.datetime.now()
        date_path = date_str.strftime("%Y-%m-%d_%H-%M-%S")
        cur_dir = os.path.dirname(os.path.abspath('__file__'))
        excel_dir = os.path.join(cur_dir, 'excel', date_path)

        if os.path.exists(excel_dir):
            shutil.rmtree(excel_dir)
        os.makedirs(excel_dir)
        self.file_path = os.path.join('excel', date_path, 'oracle_objects_statistic.xls')
    
    def __excel_save(self):
        self.book.save(self.file_path)