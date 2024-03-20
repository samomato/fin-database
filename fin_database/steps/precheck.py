import os.path
from datetime import timedelta
from datetime import datetime
import pandas as pd

# from fin_database.steps.step import Step
# from fin_database.steps.step import StepException
from fin_database.settings import db_dir, db_name, f_report_dir
import sqlite3

class PreCheck():

    @staticmethod
    def daily_check(date_start, date_end, utils):
        date_list = utils.calculate_date_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(f"SELECT name FROM sqlite_master  WHERE type='table' AND name='DAILY'; ").fetchall()
        if not list_tables:
            c.execute(f'CREATE TABLE DAILY ("update_date" "TIMESTAMP", "stockID" "TEXT")')
            c.execute('CREATE INDEX "ix_DAILY_update_date_stockID" on DAILY(update_date, stockID)')
            conn.commit()

        new_date_list = []
        for date in date_list:
            date_ = datetime.strptime(date, "%Y-%m-%d")
            cursor = c.execute(f"SELECT (stockID) FROM DAILY WHERE update_date='{date_}';")
            print(cursor.fetchone())
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


    @staticmethod
    def month_check(date_start, date_end, utils):
        update_list = utils.calculate_month_period(date_start, date_end)
        # print(update_list)  # just for test
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(f"SELECT name FROM sqlite_master  WHERE type='table' AND name='MONTH_REVENUE'; ").fetchall()
        if not list_tables:
            c.execute(f'CREATE TABLE MONTH_REVENUE ("update_date" "TIMESTAMP", "stockID" "TEXT")')
            c.execute('CREATE INDEX "ix_MONTH_REVENUE_update_date_stockID" on MONTH_REVENUE(update_date, stockID)')
            conn.commit()
            # c.execute('CREATE TABLE MONTH_REVENUE ("月份")')

        new_update_list = []
        month_list = []
        for date_ in update_list:
            cursor = c.execute(f"SELECT * FROM MONTH_REVENUE WHERE update_date='{date_}';")
            if cursor.fetchone() is None:
                new_update_list.append(date_)
                month_list.append([date_, (date_ - timedelta(days=30)).strftime('%Y-%m')])
            else:
                print(date_, 'already exist in DB')

        if not new_update_list:
            keep_run = False
        else:
            keep_run = True
        output = {
            'month_list': month_list,
            'keep_run': keep_run,
            'conn': conn,
            'c': c,
        }
        return output

    @staticmethod
    def f_report_check(date_start, date_end, utils):
        season_list, update_list = utils.calculate_season_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(
            f"SELECT name FROM sqlite_master  WHERE type='table' AND name='BALANCE'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE BALANCE ("update_date" "TIMESTAMP", "stockID" "TEXT", "季別" "TEXT")')
            c.execute('CREATE INDEX "ix_BALANCE_update_date_stockID" on BALANCE(update_date, stockID)')
        list_tables = c.execute(
            f"SELECT name FROM sqlite_master  WHERE type='table' AND name='CASH_FLOW'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE CASH_FLOW ("update_date" "TIMESTAMP", "stockID" "TEXT", "季別" "TEXT")')
            c.execute('CREATE INDEX "ix_CASH_FLOW_update_date_stockID" on CASH_FLOW(update_date, stockID)')
        list_tables = c.execute(
            f"SELECT name FROM sqlite_master  WHERE type='table' AND name='INCOME'; ").fetchall()
        if not list_tables:
            c.execute('CREATE TABLE INCOME ("update_date" "TIMESTAMP", "stockID" "TEXT", "季別" "TEXT")')
            c.execute('CREATE INDEX "ix_INCOME_update_date_stockID" on INCOME(update_date, stockID)')

        new_update_list = []
        new_season_list = []
        for season, date_ in zip(season_list, update_list):
            year, _ = season.split('-')
            # cursor = c.execute(f"SELECT * FROM BALANCE WHERE 季別='{season}';")  # 再改成date_
            if int(year) < 2013:
                print(year, 'must be later than 2012')
            # elif len(cursor.fetchall()) < 800:
            else:
                new_update_list.append(date_)
                new_season_list.append(season)
                utils.make_dir(f_report_dir)
                utils.make_dir(f_report_dir + f'/{season}')
            # else:  # 這邊也要改
            #     print(season, 'already exist in DB')

        if not new_season_list:
            keep_run = False
        else:
            keep_run = True
        output = {
            'season_list': new_season_list,
            'update_list': new_update_list,
            'keep_run': keep_run,
            'conn': conn,
            'c': c,
        }
        return output

    @staticmethod
    def futures_check(date_start, date_end, utils):
        date_list = utils.calculate_date_period(date_start, date_end)
        utils.make_dir(db_dir)
        db_path = os.path.join(db_dir, db_name)
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        list_tables = c.execute(f"SELECT name FROM sqlite_master  WHERE type='table' AND name='FUTURES'; ").fetchall()
        if not list_tables:
            c.execute(f'CREATE TABLE FUTURES ("update_date" "TIMESTAMP", "product" "TEXT", "法人" "TEXT")')
            c.execute('CREATE INDEX "ix_FUTURES_update_date_product" on FUTURES(update_date, product, 法人)')
            conn.commit()

        new_date_list = []
        for date in date_list:
            date_ = datetime.strptime(date, "%Y-%m-%d")
            cursor = c.execute(f"SELECT * FROM FUTURES WHERE update_date='{date_}';")
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
