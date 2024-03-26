from datetime import date
from fin_database.pipeline import Pipeline


def main():
    date_start = date(2024, 3, 19)
    date_end = date(2024, 3, 22)
    dtype = 'daily'
    # dtype = 'month'
    # dtype = 'f_report'
    # dtype = 'futures'
    Pipeline().produce(date_start, date_end, dtype)


if __name__ == '__main__':
    main()
