from datetime import date
from fin_database.pipeline import Pipeline


def main():
    date_start = date(2013, 1, 1)
    date_end = date(2024, 4, 10)
    # dtype = 'daily'
    # dtype = 'month'
    dtype = 'f_report'
    # dtype = 'futures'
    # dtype = 'taiex'
    # dtype = 'taiex_tr'
    # dtype = 'tw50i'
    # dtype = 'tw100i'
    # dtype = 'sp500tr'
    # dtype = 'vti'
    # dtype = 'vix'
    Pipeline().produce(date_start, date_end, dtype)


if __name__ == '__main__':
    main()
