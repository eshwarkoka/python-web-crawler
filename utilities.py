import os
import sys


# check if virtual environment is activated or not
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