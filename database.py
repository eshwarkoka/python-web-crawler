# database.py
# database utilities to handle database queries


import sqlite3
import settings
import utilities


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
        return True
    except sqlite3.Error as error_message:
        print(
            "Error Creating new Table \n"
            "Error Message: "+str(error_message)
        )


def get_all_links_from_database(cursor: sqlite3.Cursor):
    try:
        sql_query = """
                        SELECT link
                        FROM Links
                    """
        cursor.execute(sql_query)
        links = cursor.fetchall()
        parsed_links = []
        for link in links:
            parsed_links.append(link[0])
        return parsed_links
    except Exception as error_message:
        print(
            "Error Message: "+str(error_message)
        )


def check_if_link_exists(cursor: sqlite3.Cursor, link: str):
    sql_query = """
                    SELECT COUNT(link)
                    FROM Links
                    WHERE link = ?
                """
    cursor.execute(sql_query,(link,))
    if cursor.fetchone()[0] == 0:
        return False
    return True


def is_crawled(cursor: sqlite3.Cursor, link: str):
    try:
        sql_query = """
                        SELECT is_crawled
                        FROM Links
                        WHERE link = ?
                    """
        cursor.execute(sql_query, (link,))
        if cursor.fetchone()[0] == "1":
            return True
        return False
    except Exception as error_message:
        print(
            "Error Message: "+str(error_message)
        )


def check_if_link_crawled_before_24hrs(cursor: sqlite3.Cursor, link: str):
    try:
        sql_query = """
                        SELECT last_crawl_date
                        FROM Links
                        WHERE link = ?
                    """
        cursor.execute(sql_query,(link,))
        link_last_crawl_epoch = cursor.fetchone()[0]
        current_epoch_time = utilities.get_epoch_time()
        return utilities.check_if_epoch_difference_less_than_24hrs(epoch_1=link_last_crawl_epoch, epoch_2=current_epoch_time)
    except Exception as error_message:
        print(
            "Error Message: "+str(error_message)
        )


def insert_link_into_database(cursor: sqlite3.Cursor, link, source_link, is_crawled, last_crawl_date, response_status_code, response_content_type, response_content_length, file_path, link_created_date):
    try:
        sql_query = """
                        INSERT INTO 
                        Links(link, source_link, is_crawled, last_crawl_date, response_status_code, response_content_type, response_content_length, file_path, link_created_date) 
                        VALUES(?,?,?,?,?,?,?,?,?)
                    """
        cursor.execute(sql_query, (link, source_link, is_crawled, last_crawl_date, response_status_code, response_content_type, response_content_length, file_path, link_created_date))
        return True
    except sqlite3.Error as error_message:
        print(
            "Unable to insert data into database...\n"
            "Error message: "+str(error_message)
        )


def update_link_in_the_database(cursor: sqlite3.Cursor, link, source_link, is_crawled, last_crawl_date, response_status_code, response_content_type, response_content_length, file_path):
    try:
        sql_query = """
                        UPDATE Links 
                        SET source_link = ?,
                        is_crawled = ?,
                        last_crawl_date = ?,
                        response_status_code = ?,
                        response_content_type = ?,
                        response_content_length = ?,
                        file_path = ? 
                        WHERE link = ?
                    """
        cursor.execute(sql_query, (source_link, is_crawled, last_crawl_date, response_status_code, response_content_type, response_content_length, link))
        return True
    except sqlite3.Error as error_message:
        print(
            "Unable to update the database...\n"
            "Error message: "+str(error_message)
        )


def check_if_number_of_rows_less_than_5000(cursor: sqlite3.Cursor):
    try:
        sql_query = """
                        SELECT count(*)
                        FROM Links
                    """
        cursor.execute(sql_query)
        number_of_rows = cursor.fetchone()[0]
        if number_of_rows < 500:
            return True
        return False
    except Exception as error_message:
        print(
            "Error Message: "+str(error_message)
        )


def commit_and_close_connection(connection: sqlite3.Connection):
    connection.commit()
    connection.close()
    return


if __name__ == '__main__':
    DATABASE_NAME = settings.DATABASE_NAME
    CONNECTION = create_connection(db_file_name=DATABASE_NAME)
    CURSOR = create_cursor(connection=CONNECTION)
    # create links table if it does not exist
    create_links_table_sql_query = """
                                    CREATE TABLE IF NOT EXISTS Links
                                    (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        link TEXT,
                                        source_link TEXT,
                                        is_crawled BOOLEAN NOT NULL CHECK (is_crawled IN (0,1)),
                                        last_crawl_date INTEGER,
                                        response_status_code INTEGER,
                                        response_content_type TEXT,
                                        response_content_length INTEGER,
                                        file_path TEXT,
                                        link_created_date INTEGER
                                    );
                                    """
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
    commit_and_close_connection(connection=CONNECTION)