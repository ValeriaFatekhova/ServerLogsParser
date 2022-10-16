import json
import os
import re
import sqlite3
from pprint import pprint


def create_db():
    connection = sqlite3.connect("logs_database.db")
    return connection


def create_db_table(connection, table_name):
    connection.cursor().execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        (
            id INTEGER PRIMARY KEY,
            ip TEXT NOT NULL,
            data TEXT NOT NULL,
            timezone TEXT NOT NULL,
            method TEXT NOT NULL,
            content TEXT NOT NULL,
            version TEXT NOT NULL,
            code TEXT NOT NULL,
            size TEXT NOT NULL, 
            url TEXT NOT NULL, 
            header TEXT NOT NULL, 
            duration TEXT NOT NULL
        );
        """)


def insert_into_db_table(connection, table, data):
    connection.cursor().execute(f"""
        INSERT INTO {table} 
        (ip, data, timezone, method, content, version, code, size, url, header, duration) 
        VALUES {data};
        """)
    connection.commit()


def delete_table(connection, table):
    connection.cursor().execute(f"""
            DROP TABLE IF EXISTS {table};
            """)
    connection.commit()


def string_parser(string):
    """Разбирает строку лога на словарь"""

    r = r"(?P<ip>.*?) (.*?) \[(?P<data>.*?) (?P<timezone>.*?)\] \"(?P<method>[A-Z]+) (?P<content>.*?) (?P<version>.*?)\" (?P<code>\d+) (?P<size>[\d|-]+) \"(?P<url>.*?)\" \"(?P<header>.*?)\" (?P<duration>\d+)"
    m = re.match(r, string)
    request = dict()
    request["ip"] = m.group("ip")
    request["data"] = m.group("data")
    request["timezone"] = m.group("timezone")
    request["method"] = m.group("method")
    request["content"] = m.group("content").replace("'", '"')
    request["version"] = m.group("version").replace("'", '"')
    request["code"] = m.group("code")
    request["size"] = m.group("size")
    request["url"] = m.group("url")
    request["header"] = m.group("header").replace("'", '"')
    request["duration"] = m.group("duration")

    return request


def log_parser(log_file_path):
    """Парсинг файла с логами"""

    # создаем базу данных
    connection = create_db()
    table = 'log_'+log_file_path.split("\\")[-1].split(".")[0]
    create_db_table(connection, table)

    # парсим лог построчно и каждую строку записываем в базу
    with open(log_file_path) as f:
        string = f.readline()
        while string != "":
            request = string_parser(string)
            print(request["data"], request["header"])
            insert_into_db_table(connection, table, tuple(request.values()))
            string = f.readline()

    # формируем словарь результат
    res = dict()
    res["log file"] = log_file_path
    res["requests number"] = connection.cursor().execute(f"""
            SELECT COUNT(id) FROM {table};
            """).fetchone()[0]

    res["methods"] = dict(connection.cursor().execute(f"""
            SELECT method, COUNT(method) AS nums
            FROM {table}
            GROUP BY method
            ORDER BY nums DESC;
            """).fetchall())

    res["popular hosts"] = dict(connection.cursor().execute(f"""
            SELECT ip, COUNT(ip) AS nums
            FROM {table}
            GROUP BY ip
            ORDER BY nums DESC
            LIMIT 3;
            """).fetchall())

    res["longest requests"] = []
    longest_requests = connection.cursor().execute(f"""
            SELECT ip, data, timezone, method, content, version, code, size, url, header, duration
            FROM {table}            
            ORDER BY duration DESC
            LIMIT 3;
            """).fetchall()
    for request in longest_requests:
        res["longest requests"].append(dict(zip(('ip', 'data', 'timezone', 'method', 'content', 'version', 'code', 'size', 'url', 'header', 'duration'), request)))

    # удаляем таблицу из базы
    delete_table(connection, table)
    connection.close()

    return res


def print_json(data):
    """Печатает словарь"""
    pprint(data)


def save_to_file(data, file_path):
    """Пишет словарь в файл как джейсон объект"""
    with open(file_path, "w") as f:
        f.write(json.dumps(data, indent=4))


if __name__ == "__main__":
    path = input("Введите путь к логфайлу или каталогу с логфайлами: ")
    path = path.split()

    if len(path) == 1:
        data = log_parser(path[0])
        save_to_file(data, path[0].split(".")[0]+".json")
        print_json(data)

    if len(path) == 2 and path[1] == "--all":
        for file in os.listdir(path[0]):
            print(os.path.join(path[0], file))
            data = log_parser(os.path.join(path[0], file))
            save_to_file(data, os.path.join(path[0], file.split(".")[0]+".json"))
            print_json(data)
