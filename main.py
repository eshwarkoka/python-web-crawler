# main.py
# main file to be executed to start the web crawler


import sys
import time
import requests
import utilities
import settings
import database
from bs4 import BeautifulSoup


def start_crawler():
    while True:
        DATABASE_NAME = settings.DATABASE_NAME
        CONNECTION = database.create_connection(db_file_name=DATABASE_NAME)
        CURSOR = database.create_cursor(connection=CONNECTION)
        # get all the links from the database
        links = database.get_all_links_from_database(cursor=CURSOR)
        for link in links:
            # check if link is already crawled
            if database.is_crawled(cursor=CURSOR, link=link):
                # check if link is crawled in the last 24 hrs
                if database.check_if_link_crawled_before_24hrs(cursor=CURSOR, link=link):
                    # it True, do nothing!
                    print("nothing!")
                    pass
                else:
                    # link is not crawled in last 24 hrs
                    parser = 'html.parser'
                    response = requests.get(link)
                    response_status_code = response.status_code
                    response_text = response.text
                    response_content_length = len(response_text)
                    response_headers = response.headers
                    response_content_type = response_headers['Content-Type']
                    if response_status_code == 200:
                        bs = BeautifulSoup(response_text, parser)
                        for a in bs.find_all('a'):
                            href = a.get('href')
                            if utilities.href_is_valid(href=href):
                                source_link = link
                                full_link = utilities.get_full_link(source_link=source_link, link=href)
                                CONNECTION = database.create_connection(db_file_name=DATABASE_NAME)
                                CURSOR = database.create_cursor(connection=CONNECTION)
                                # if limit not exceeded
                                if database.check_if_number_of_rows_less_than_5000(cursor=CURSOR):
                                    # update the link in the database
                                    database.update_link_in_the_database(
                                        cursor = CURSOR,
                                        link = full_link,
                                        source_link = source_link,
                                        is_crawled = 1,
                                        last_crawl_date = utilities.get_epoch_time(),
                                        response_status_code = response_status_code,
                                        response_content_type = response_content_type,
                                        response_content_length = response_content_length,
                                        file_path = "abcd"
                                    )
                                    database.commit_and_close_connection(connection=CONNECTION)
                                    print("link updated...")
                                else:
                                    print("Maximum limit reached!")
                                    print("sleeping for 5 seconds...")
                                    time.sleep(5)
            else:
                # link is not crawled at all
                parser = 'html.parser'
                response = requests.get(link)
                response_status_code = response.status_code
                response_text = response.text
                response_content_length = len(response_text)
                response_headers = response.headers
                response_content_type = response_headers['Content-Type']
                if response_status_code == 200:
                    bs = BeautifulSoup(response_text, parser)
                    for a in bs.find_all('a'):
                        href = a.get('href')
                        if utilities.href_is_valid(href=href):
                            source_link = link
                            full_link = utilities.get_full_link(source_link=source_link, link=href)
                            CONNECTION = database.create_connection(db_file_name=DATABASE_NAME)
                            CURSOR = database.create_cursor(connection=CONNECTION)
                            # if limit not exceeded
                            if database.check_if_number_of_rows_less_than_5000(cursor=CURSOR):
                                # insert the link in the database
                                database.insert_link_into_database(
                                    cursor = CURSOR,
                                    link = full_link,
                                    source_link = source_link,
                                    is_crawled = 1,
                                    last_crawl_date = utilities.get_epoch_time(),
                                    response_status_code = response_status_code,
                                    response_content_type = response_content_type,
                                    response_content_length = response_content_length,
                                    file_path = "abcd",
                                    link_created_date = utilities.get_epoch_time()
                                )
                                database.commit_and_close_connection(connection=CONNECTION)
                                print("link inserted...")
                            else:
                                print("Maximum limit reached!")
                                print("sleeping for 5 seconds...")
                                time.sleep(5)
        # sleep for 5 seconds
        print("sleeping for 5 seconds...")
        time.sleep(5)


if __name__ == '__main__':
    if utilities.check_venv():
        start_crawler()