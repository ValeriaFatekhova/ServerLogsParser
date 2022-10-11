import json
import os
import re
from pprint import pprint


def string_parser(string):
    """Разбирает строку лога на словарь"""

    r = r"(?P<ip>.*?) (.*?) \[(?P<data>.*?) (?P<timezone>.*?)\] \"(?P<method>[A-Z]+) (?P<content>.*?) (?P<version>.*?)\" (?P<code>\d+) (?P<size>[\d|-]+) \"(?P<url>.*?)\" \"(?P<header>.*?)\" (?P<duration>\d+)"
    m = re.match(r, string)
    request = dict()
    request["ip"] = m.group("ip")
    request["data"] = m.group("data")
    request["timezone"] = m.group("timezone")
    request["method"] = m.group("method")
    request["content"] = m.group("content")
    request["version"] = m.group("version")
    request["code"] = m.group("code")
    request["size"] = m.group("size")
    request["url"] = m.group("url")
    request["header"] = m.group("header")
    request["duration"] = m.group("duration")

    return request


def log_parser(log_file_path):
    """Парсинг файла с логами"""

    hosts = dict()
    num_requests = 0
    http_methods = {"GET": 0, "POST": 0, "HEAD": 0, "PUT": 0, "PATCH": 0, "DELETE": 0, "CONNECT": 0, "OPTIONS": 0,
                    "TRACE": 0}
    longtime_requests = [{"duration": 0}, {"duration": 0}, {"duration": 0}]

    with open(log_file_path) as f:
        string = f.readline()
        while string != "":
            request = string_parser(string)
            num_requests += 1  # считаем общее число запросов

            # формируем словарь, где ключи - хосты, а значения - чилсо обращений к хосту-ключу
            if request["ip"] in hosts:
                hosts[request["ip"]] += 1
            else:
                hosts[request["ip"]] = 1

            # формируем словарь, где ключи - http методы, а значения - число вызовов метода-ключа
            try:
                http_methods[request["method"]] += 1
            except KeyError:
                http_methods[request["method"]] = 1

            # формируем список трех самых длительных запросов
            if int(request["duration"]) > int(longtime_requests[0]["duration"]):
                longtime_requests[0], longtime_requests[1], longtime_requests[2] = request, longtime_requests[0], longtime_requests[1]
            elif int(request["duration"]) <= int(longtime_requests[0]["duration"]) and int(request["duration"]) > int(longtime_requests[1]["duration"]):
                longtime_requests[1], longtime_requests[2] = request, longtime_requests[1]
            elif int(request["duration"]) <= int(longtime_requests[1]["duration"]) and int(request["duration"]) > int(longtime_requests[2]["duration"]):
                longtime_requests[2] = request

            string = f.readline()

    # формируем словарь результат
    res = dict()
    res["log file"] = log_file_path
    res["requests number"] = num_requests
    res["methods"] = http_methods

    # sorted_hosts = dict()
    # sorted_keys = sorted(hosts, key=hosts.get, reverse=True)
    # for key in sorted_keys:
    #   sorted_hosts[key] = hosts[key]

    # сортируем ключи словаря по возрастанию значений по данным ключам, берем три первых - это хосты с наибольшим числом обращений
    res["popular hosts"] = sorted(hosts, key=hosts.get, reverse=True)[:3]
    res["longest requests"] = longtime_requests

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
