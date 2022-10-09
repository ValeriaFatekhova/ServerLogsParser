import os


def log_parser(log_file_path):
    res = dict()
    with open(log_file_path) as f:
        f.readline()

    return res


def print_json(json):
    print(json)


path = input("Введите путь к логфайлу или каталогу с логфайлами: ")

for file in os.listdir(path):
    print(os.path.join(path, file))
    print_json(log_parser(os.path.join(path, file)))


