import os.path
from fin_database.steps.step import Step
from fin_database.steps.step import StepException
from fin_database.settings import db_dir, db_name
import sqlite3

class PreFlight(Step):

    def daily_process(self, input_, utils):
        date_list = utils.calculate_date_period(input_['date_start'], input_['date_end'])
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
        print(new_date_list)
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
        return output, utils

    def month_process(self, input_, utils):
        print("")

    def f_report_process(self):
        print("")

    def futures_process(self):
        print("")
