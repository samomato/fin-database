from fin_database.steps.step import Step


class Storer(Step):

    def daily_process(self, input_, utils):
        cn_list = ['日期']
        [cn_list.append(_) for _ in input_['data'].columns]
        input_['c'].execute('PRAGMA TABLE_INFO(DAILY)')
        columns_of_tables = [tup[1] for tup in input_['c'].fetchall()]
        if cn_list != columns_of_tables:
            for cn in cn_list:
                if cn not in columns_of_tables:
                    input_['c'].execute(f"ALTER TABLE 'DAILY' ADD COLUMN {cn}")
                    print(f'Add new column {cn} to DB from {input_["date"]}')
            input_['conn'].commit()
        input_['data'].to_sql('DAILY', input_['conn'], if_exists='append')

        return input_

    def month_process(self, input_, utils):
        pass

    def f_report_process(self):
        pass

    def futures_process(self):
        pass