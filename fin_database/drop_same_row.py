import sqlite3
import os
from fin_database.settings import db_name


def drop_duplicate_in_db(db_directory, db_file_name, table_):
    db_path = os.path.join(db_directory, db_file_name)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(f"CREATE TABLE temp_table as SELECT DISTINCT * FROM {table_}")
    conn.commit()
    c.execute(f"DELETE FROM {table_}")
    conn.commit()
    c.execute(f"INSERT INTO {table_} SELECT * FROM temp_table")
    conn.commit()
    c.execute("DROP TABLE temp_table")
    conn.commit()
    conn.close()
    return


def main():
    db_dir = '../findata'
    table = "DAILY"
    drop_duplicate_in_db(db_dir, db_name, table)


if __name__ == '__main__':
    main()

