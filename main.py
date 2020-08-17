# main.py
# main file to be executed to start the web crawler


import sys
import requests
import utilities
import settings
import database
from bs4 import BeautifulSoup


def scrape_link(link: str):
    ROOT_URL = settings.ROOT_URL
    FULL_LINK = str(ROOT_URL)+str(link)
    DATABASE_NAME = settings.DATABASE_NAME
    CONNECTION = database.create_connection(db_file_name=DATABASE_NAME)
    CURSOR = database.create_cursor(connection=CONNECTION)
    if database.check_if_link_exists(cursor=CURSOR, link=FULL_LINK):
        # if link exists in the database
        if database.check_if_link_crawled_before_24hrs(cursor=CURSOR, link=FULL_LINK):
            # if link crawled before 24 hrs, then do nothing
            return
        else:
            # if link is not crawled since 24 hrs
            parser = 'html.parser'
            response = requests.get(FULL_LINK)
            response = requests.get(ROOT_URL)
            response_status_code = response.status_code
            response_text = response.text
            response_content_length = len(response_text)
            response_headers = response.headers
            response_content_type = response_headers['Content-Type']
            if response_status_code == 200:
                bs = BeautifulSoup(response_text, parser)
                for link in bs.find_all('a'):
                    if utilities.href_is_valid(href=link.get('href')):
                        # recursive call
                        # print("update recursive call")
                        scrape_link(link=link)
                        CONNECTION = database.create_connection(db_file_name=DATABASE_NAME)
                        CURSOR = database.create_cursor(connection=CONNECTION)
                        # if limit not exceeded
                        if database.check_if_number_of_rows_less_than_5000(cursor=CURSOR):
                            # update the link in the database
                            database.update_link_in_the_database(
                                cursor = CURSOR,
                                link = link,
                                source_link = FULL_LINK,
                                is_crawled = 1,
                                last_crawl_date = utilities.get_epoch_time(),
                                response_status_code = response_status_code,
                                response_content_type = response_content_type,
                                response_content_length = response_content_length
                            )
                            database.commit_and_close_connection(connection=CONNECTION)
                        else:
                            print("Maximum limit reached!")
                            sys.exit()
    else:
        # if link does not exist in the database
        parser = 'html.parser'
        response = requests.get(FULL_LINK)
        response = requests.get(ROOT_URL)
        response_status_code = response.status_code
        response_text = response.text
        response_content_length = len(response_text)
        response_headers = response.headers
        response_content_type = response_headers['Content-Type']
        if response_status_code == 200:
            bs = BeautifulSoup(response_text, parser)
            for link in bs.find_all('a'):
                if utilities.href_is_valid(href=link.get('href')):
                    # recursive call
                    # print("insert recursive call")
                    scrape_link(link=link)
                    CONNECTION = database.create_connection(db_file_name=DATABASE_NAME)
                    CURSOR = database.create_cursor(connection=CONNECTION)
                    # if limit not exceeded
                    if database.check_if_number_of_rows_less_than_5000(cursor=CURSOR):
                        # insert the link into the database
                        database.insert_link_into_database(
                            cursor = CURSOR,
                            link = link,
                            source_link = FULL_LINK,
                            is_crawled = 1,
                            last_crawl_date = utilities.get_epoch_time(),
                            response_status_code = response_status_code,
                            response_content_type = response_content_type,
                            response_content_length = response_content_length,
                            link_created_date = utilities.get_epoch_time()
                        )
                        database.commit_and_close_connection(connection=CONNECTION)
                    else:
                        print("Maximum limit reached!")
                        sys.exit()


if __name__ == '__main__':
    if utilities.check_venv():
        scrape_link('/')
        print("DONE")