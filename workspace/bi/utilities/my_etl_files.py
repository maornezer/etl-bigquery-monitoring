import json
import os
from pathlib import Path



# functions
def readFile(fileName):
    with open(fileName, "r") as file:
        data = file.read()
        file.close()
    return data


def writeFile(fileName, Data):
    with open(fileName, "w", newline='', encoding='utf-8') as file:
        file.write(Data)
    file.close()


def readJsonFile(fileName):
    try:
        with open(fileName, "rb") as file:
            return json.load(file)
    except IOError as err:
        if err.errno == 2:  # File didn't exist, no biggie
            return {}
        else:  # Something weird, just re-raise the error
            raise


def ensureDirectory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def header(stirng):
    x = len(stirng)*"="
    print(f"{stirng}\n{x}\n")


def get_paths(root, home, file_name, repo_tail):
    bi_path = home / f'{root}/'
    bi_auth = home / 'auth/'
    data_path = home / f'temp/data/{root}/{repo_tail}'
    logs_path = home / f'temp/logs/{root}/{repo_tail}'
    folder_name = Path(os.path.dirname(file_name))
    return bi_path, bi_auth, data_path, logs_path, folder_name