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

    def make_dir(self, path):
        if not os.path.isdir(path):
            os.mkdir(path)
