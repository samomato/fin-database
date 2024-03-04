from datetime import date
from fin_database.pipeline import Pipeline



def main():
    date_start = date(2020, 4, 10)
    date_end = date(2020, 5, 30)
    # dtype = 'daily'
    # dtype = 'month'
    dtype = 'f_report'
    Pipeline().produce(date_start, date_end, dtype)


if __name__ == '__main__':
    main()
