import os
import sys
import requests
from bs4 import BeautifulSoup


def testing():
    ROOT_URL = "https://flinkhub.com/"
    parser = 'html.parser'
    response = requests.get(ROOT_URL)
    response_status_code = response.status_code
    response_text = response.text
    response_content_length = len(response_text)
    response_headers = response.headers
    print(response_headers)
    response_content_type = response_headers['Content-Type']
    if response_status_code == 200:
        bs = BeautifulSoup(response_text, parser)
        for link in bs.find_all('a'):
            link = link.get('href')
            if link == "" or link == "/" or link == "#" or link == "javascript:;":
                pass
            else:
                print(link)


if __name__ == '__main__':
    check_venv()
    testing()