from fin_database.steps.step import Step
from fin_database.settings import db_dir, db_name, f_report_dir


class Storer(Step):
    def daily_process(self, input_, utils):
        cn_list = ['update_date', 'stockID']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(DAILY)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'DAILY' ADD COLUMN {cn} 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["date"]}')
            input_['conn'].commit()
        input_['data'].to_sql('DAILY', input_['conn'], if_exists='append')

        return

    def month_process(self, input_, utils):
        # cn_list = ['月份']
        cn_list = ['update_date', 'stockID']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(MONTH_REVENUE)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'MONTH_REVENUE' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["month"]}')
            input_['conn'].commit()
        input_['data'].to_sql('MONTH_REVENUE', input_['conn'], if_exists='append')

        return input_

    def f_report_process(self, input_, utils):

        cn_list = []
        [cn_list.append(_) for _ in input_['data'][0].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(BALANCE)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'BALANCE' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["season"]}')
            input_['conn'].commit()
        input_['data'][0].to_sql('BALANCE', input_['conn'], if_exists='append', index=False)

        cn_list = []
        [cn_list.append(_) for _ in input_['data'][1].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(INCOME)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'INCOME' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["season"]}')
            input_['conn'].commit()
        input_['data'][1].to_sql('INCOME', input_['conn'], if_exists='append', index=False)

        cn_list = []
        [cn_list.append(_) for _ in input_['data'][2].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(CASH_FLOW)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'CASH_FLOW' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["season"]}')
            input_['conn'].commit()
        input_['data'][2].to_sql('CASH_FLOW', input_['conn'], if_exists='append', index=False)

        return

    def futures_process(self, input_, utils):
        cn_list = ['update_date', 'product', '法人']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(FUTURES)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'FUTURES' ADD COLUMN {cn} 'REAl'")
                    print(f'Add new column {cn} to DB from {input_["date"]}')
            input_['conn'].commit()
        input_['data'].to_sql('FUTURES', input_['conn'], if_exists='append')
        return 0

    def taiex_process(self, input_, utils):
        cn_list = ['update_date']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(TAIEX)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'TAIEX' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["month"]}')
            input_['conn'].commit()
        input_['data'].to_sql('TAIEX', input_['conn'], if_exists='append')
        utils.drop_duplicate_in_db(db_dir, db_name, 'TAIEX')
        return 0

    def taiex_tr_process(self, input_, utils):
        cn_list = ['update_date']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(TAIEX_TR)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'TAIEX_TR' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["month"]}')
            input_['conn'].commit()
        input_['data'].to_sql('TAIEX_TR', input_['conn'], if_exists='append')
        utils.drop_duplicate_in_db(db_dir, db_name, 'TAIEX_TR')
        return 0

    def tw50i_process(self, input_, utils):
        cn_list = ['update_date']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(TW50I)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'TW50I' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["month"]}')
            input_['conn'].commit()
        input_['data'].to_sql('TW50I', input_['conn'], if_exists='append')
        utils.drop_duplicate_in_db(db_dir, db_name, 'TW50I')
        return 0

    def tw100i_process(self, input_, utils):
        cn_list = ['update_date']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(TW100I)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'TW100I' ADD COLUMN '{cn}' 'REAL'")
                    print(f'Add new column {cn} to DB from {input_["month"]}')
            input_['conn'].commit()
        input_['data'].to_sql('TW100I', input_['conn'], if_exists='append')
        utils.drop_duplicate_in_db(db_dir, db_name, 'TW100I')
        return 0