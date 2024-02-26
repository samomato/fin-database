import os.path
# from fin_database.steps.step import Step
# from fin_database.steps.step import StepException
from fin_database.settings import db_dir, db_name
import sqlite3

class PreCheck():

    def daily_check(self, date_start, date_end, utils):
        date_list = utils.calculate_date_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(f"SELECT name FROM sqlite_master  WHERE type='table' AND name='DAILY'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE DAILY ("日期")')

        new_date_list = []
        for date in date_list:
            cursor = c.execute(f"SELECT * FROM DAILY WHERE 日期='{date}';")
            if cursor.fetchone() is None:
                new_date_list.append(date)
            else:
                print(date, 'already exist in DB')

        if not new_date_list:
            keep_run = False
        else:
            keep_run = True
        output = {
            'date_list': new_date_list,
            'keep_run': keep_run,
            'conn': conn,
            'c': c,
        }
        return output


    def month_check(self, date_start, date_end, utils):
        month_list = utils.calculate_month_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(f"SELECT name FROM sqlite_master  WHERE type='table' AND name='MONTH_REVENUE'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE MONTH_REVENUE ("月份")')

        new_month_list = []
        for month in month_list:
            cursor = c.execute(f"SELECT * FROM MONTH_REVENUE WHERE 月份='{month}';")
            if cursor.fetchone() is None:
                new_month_list.append(month)
            else:
                print(month, 'already exist in DB')

        if not new_month_list:
            keep_run = False
        else:
            keep_run = True
        output = {
            'month_list': new_month_list,
            'keep_run': keep_run,
            'conn': conn,
            'c': c,
        }
        return output

    def f_report_check(self):
        print("")

    def futures_check(self):
        print("")
