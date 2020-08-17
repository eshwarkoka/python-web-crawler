# utilities.py
# all general utilities or functions are added here


import os
import sys
import time


# checks if virtual environment is activated or not
def check_venv():
    project_path = os.getcwd()
    venv_path = '/venv/bin/python'
    python_executable = sys.executable
    if python_executable == project_path+venv_path:
        return True
    else:
        print("** Virtual Environment is not activated **")
        print("""** run "source venv/bin/activate" to activate virtual environment  **""")
        sys.exit()


# returns epoch time 
def get_epoch_time():
    return int(time.time())


# checks if epoch difference is less than 24 hrs or not
def check_if_epoch_difference_less_than_24hrs(epoch_1, epoch_2):
    # epoch_1 < epoch_2
    if (int(epoch_2) - int(epoch_1)) < 86400:
        return True
    return False


# checks if href in anchor tag is valid or not
def href_is_valid(href):
    if href == "" or href == "/" or "#" in href or href == "javascript:;":
        return False
    return True


# checks link and returns full link
def get_full_link(source_link: str,link: str):
    if link.startswith("https://"):
        return str(link)
    elif link.startswith("/"):
        return str(source_link+link)
    