from datetime import date
from fin_database.pipeline import Pipeline


def main():
    date_start = date(1999, 1, 1)
    date_end = date(2024, 4, 10)
    # dtype = 'daily'
    # dtype = 'month'
    # dtype = 'f_report'
    # dtype = 'futures'
    dtype = 'taiex'
    Pipeline().produce(date_start, date_end, dtype)


if __name__ == '__main__':
    main()
