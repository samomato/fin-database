from datetime import date
from fin_database.pipeline import Pipeline



def main():
    date_start = date(2020, 3, 14)
    date_end = date(2020, 8, 27)
    # dtype = 'daily'
    dtype = 'month'
    Pipeline().produce(date_start, date_end, dtype)


if __name__ == '__main__':
    main()
