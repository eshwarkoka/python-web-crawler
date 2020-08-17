# database.py
# create database utilities


import sqlite3
import settings


def create_connection(db_file_name) -> sqlite3.Connection:
    URI = "file:"+db_file_name+"?mode=rw"
    connection = None
    try:
        connection = sqlite3.connect(URI,uri=True)
    except sqlite3.Error as error_message:
        print("Error connecting to database...\n"
              "database name: "+db_file_name+"\n"
              "error message: "+str(error_message))
    return connection


def create_cursor(connection: sqlite3.Connection) -> sqlite3.Cursor:
    return connection.cursor()


def create_table(cursor: sqlite3.Cursor, create_table_sql_query):
    try:
        cursor.execute(create_table_sql_query)
    except sqlite3.Error as error_message:
        print(
            "Error Creating new Table \n"
            "Error Message: "+str(error_message)
        )


def check_if_link_exists(cursor: sqlite3.Cursor, link: str):
    sql_query = """
                    SELECT COUNT(link) from Links where link = ?
                """
    cursor.execute(sql_query,(link,))
    if cursor.fetchone()[0] == 0:
        return False
    return True


if __name__ == '__main__':
    DATABASE_NAME = settings.DATABASE_NAME
    CONNECTION = create_connection(db_file_name=DATABASE_NAME)
    CURSOR = create_cursor(connection=CONNECTION)
    create_links_table_sql_query = """
                                    CREATE TABLE IF NOT EXISTS Links
                                    (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        link TEXT NOT NULL,
                                        source_link TEXT NOT NULL,
                                        is_crawled BOOLEAN NOT NULL CHECK (is_crawled IN (0,1)),
                                        last_crawl_date INTEGER NOT NULL,
                                        response_status_code INTEGER NOT NULL,
                                        response_content_type TEXT,
                                        resonse_content_length INTEGER NOT NULL,
                                        link_created_date INTEGER NOT NULL
                                    );
                                    """
    # create Links table
    try:
        create_table(cursor=CURSOR, create_table_sql_query=create_links_table_sql_query)
        print(
            "database.py executed\n"
            "Links Table Created!"
        )
    except sqlite3.Error as error_message:
        print(
            "Error Creating Links Table\n"
            "Error Message: "+str(error_message)
        )
    print(check_if_link_exists(cursor=CURSOR,link="a"))
    