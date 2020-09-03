from openpyxl import Workbook
from openpyxl.styles import Alignment
import logging
import os
import datetime
import shutil
from ..models import SqliteDB

class ExportEachObjectExcel:
    """
    export statistic data to excel
    """

    def __init__(self):
        self.book = Workbook()
        self.sqlite_db = SqliteDB()
        self.__excel_file_path_config()
        self.__first_sheet_init()
        self.__excel_save()
    
    def __excel_file_path_config(self):
        """ get excel path """
        date_str = datetime.datetime.now()
        date_path = date_str.strftime("%Y-%m-%d_%H-%M-%S")
        cur_dir = os.path.dirname(os.path.abspath('__file__'))
        excel_dir = os.path.join(cur_dir, 'excel', date_path)

        if os.path.exists(excel_dir):
            shutil.rmtree(excel_dir)
        os.makedirs(excel_dir)
        self.file_path = os.path.join('excel', date_path, 'oracle_objects_statistic.xls')
    
    def __excel_save(self):
        """ save excel """
        self.book.save(self.file_path)

    def __first_sheet_init(self):
        """ write first sheet """
        cur_sheet = self.book.get_sheet_by_name(self.book.sheetnames[0])
        cur_sheet.title = 'dashboard'
        cur_sheet['A1'] = 'Objects'
        cur_sheet['B1'] = 'Owner'
        cur_sheet['C1'] = 'Source Count'
        cur_sheet['D1'] = 'Dest Count'
        cur_sheet['E1'] = 'Verify Error Count'
        objects_data = self.sqlite_db.sqlite_verify_object_statistic_query()
        self.__write_objects_statis_data_to_excel(cur_sheet, objects_data)

    def __write_objects_statis_data_to_excel(self, cur_sheet, data):
        """ write objects statistic data to first sheet """
        cur_row = int(cur_sheet.max_row) + 1

        for row in data:
            cur_sheet.cell(row=cur_row, column=1).value = row[0]
            cur_sheet.cell(row=cur_row, column=2).value = row[1].upper()
            cur_sheet.cell(row=cur_row, column=3).value = row[2]
            cur_sheet.cell(row=cur_row, column=4).value = row[3]
            cur_sheet.cell(row=cur_row, column=5).value = row[4]
            cur_row += 1
            

