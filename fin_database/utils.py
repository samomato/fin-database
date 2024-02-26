from datetime import timedelta

from datetime import date
import os


class Utils:
    def calculate_date_period(self, date_start, date_end):
        period = (date_end - date_start).days
        a_day = timedelta(days=1)

        date_list = [date_start.isoformat()]
        for i in range(period):
            date_start = date_start + a_day
            date_list.append(date_start.isoformat())
        return date_list

    def calculate_month_period(self, date_start, date_end):
        period = (date_end - date_start).days
        delta = timedelta(days=1)
        today = date.today()

        month_list = [date_start.strftime('%Y%m')]
        for i in range(int(period)):
            old_month = date_start.strftime('%Y%m')
            date_start = date_start + delta
            new_month = date_start.strftime('%Y%m')
            if new_month != old_month:
                month_list.append(new_month)
        # Can't download when before 10th of the newest month:
        if int(date_end.month) == int(today.month) and int(today.day) < 10:
            month_list.pop(-1)

        return month_list

    def make_dir(self, path):
        if not os.path.isdir(path):
            os.mkdir(path)
