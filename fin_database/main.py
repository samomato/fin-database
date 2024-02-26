from datetime import date
from fin_database.pipeline import Pipeline



def main():
    date_start = date(2020, 2, 14)
    date_end = date(2020, 2, 21)
    Pipeline().produce(date_start, date_end)


if __name__ == '__main__':
    main()
