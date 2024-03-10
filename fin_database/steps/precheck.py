import os.path
from datetime import datetime
import pandas as pd

# from fin_database.steps.step import Step
# from fin_database.steps.step import StepException
from fin_database.settings import db_dir, db_name, f_report_dir
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
            dft = pd.DataFrame({'update_date': [datetime(1900, 1, 1)],'stockID':['temp']})
            dft = dft.set_index(['update_date', 'stockID'])
            dft.to_sql('DAILY', conn, if_exists='append')

        new_date_list = []
        for date in date_list:
            cursor = c.execute(f"SELECT * FROM DAILY WHERE update_date='{date}';")
            if cursor.fetchone() is None:
                new_date_list.append(date)
            else:
                print(date, 'already exist in DAILY Table of DB')

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

    def f_report_check(self, date_start, date_end, utils):
        season_list = utils.calculate_season_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(
            f"SELECT name FROM sqlite_master  WHERE type='table' AND name='BALANCE'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE BALANCE ("季別")')
        list_tables = c.execute(
            f"SELECT name FROM sqlite_master  WHERE type='table' AND name='CASH_FLOW'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE CASH_FLOW ("季別")')
        list_tables = c.execute(
            f"SELECT name FROM sqlite_master  WHERE type='table' AND name='INCOME'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE INCOME ("季別")')

        new_season_list = []
        for season in season_list:
            year, _ = season.split('-')
            cursor = c.execute(f"SELECT * FROM BALANCE WHERE 季別='{season}';")
            if int(year) < 2013:
                print(year, 'must be later than 2012')
            elif len(cursor.fetchall()) < 790:
                new_season_list.append(season)
                utils.make_dir(f_report_dir)
                utils.make_dir(f_report_dir + f'/{season}')
            else:  # 這邊也要改
                print(season, 'already exist in DB')
        print(new_season_list)  # for test
        if not new_season_list:
            keep_run = False
        else:
            keep_run = True
        output = {
            'season_list': new_season_list,
            'keep_run': keep_run,
            'conn': conn,
            'c': c,
        }
        return output

    def futures_check(self, date_start, date_end, utils):
        date_list = utils.calculate_date_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(f"SELECT name FROM sqlite_master  WHERE type='table' AND name='FUTURES'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE FUTURES ("日期")')

        new_date_list = []
        for date in date_list:
            cursor = c.execute(f"SELECT * FROM FUTURES WHERE 日期='{date}';")
            if cursor.fetchone() is None:
                new_date_list.append(date)
            else:
                print(date, 'already exist in FUTURES Table of DB')

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
