import os
import re
import psycopg2
from datetime import datetime, date

class Log:
    '''Класс для хранения информации о логах'''
    def __init__(self):
        self.__server_ip = 'Нет данных'
        self.__date = date.today()
        self.__query = 'Нет данных'
        self.__response = 'Нет данных'
        self.__weight = 'Нет данных'

    def __repr__(self):
        return f'ip: {self.__server_ip}, date: {self.__date}, query: {self.__query}, response: {self.__response}, weight: {self.__weight}'

    def __iter__(self):
        # Прописываем итерацию для формирования кортежей
        for attr in (self.__server_ip, self.__date, self.__query, self.__response, self.__weight):
            yield attr

    @property
    def server_ip(self):
        return self.__server_ip

    @server_ip.setter
    def server_ip(self, value):
        self.__server_ip = value

    @property
    def date(self):
        return self.__date

    @date.setter
    def date(self, value):
        self.__date = value

    @property
    def query(self):
        return self.__query

    @query.setter
    def query(self, value):
        self.__query = value

    @property
    def response(self):
        return self.__response

    @response.setter
    def response(self, value):
        self.__response = value

    @property
    def weight(self):
        return self.__weight

    @weight.setter
    def weight(self, value):
        self.__weight = value

# Паттерны для извлечения данных из строк логов
data_patterns = {
    '%h': r'\b(?:\d{1,3}\.){3}\d{1,3}\b',  # IP адрес сервера
    '%t': r'(\d{2}\/[A-Za-z]{3,5}\/\d{4}:\d{2}:\d{2}:\d{2})',  # Дата и время
    '%r': r'"(.*?)"',  # Запрос
    '%>s': r'\b\d{3}\b',  # Код ответа
    '%b': r'\b\d+\b'  # Размер ответа
}

def read_data(file_paths, data_patterns):
    '''Функция для чтения логов из файла'''
    logs = []
    for file_path, file_pattern in file_paths:
        if not os.path.exists(file_path):
            print(f'Данный файл не найден: {file_path}')
            continue

        file_patterns = file_pattern.split(',')
        file_patterns = list(filter(lambda pattern: pattern in data_patterns, file_patterns))
        if not file_patterns:
            print(f'Некорректный или пустой паттерн: {file_pattern}')
            continue
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            for line in lines:
                line = line.strip()
                log = create_log(line, data_patterns, file_patterns)
                logs.append(log)
    return logs

def create_log(line, data_patterns, file_patterns):
    '''Функция для создания логов из строки с помощью паттернов'''
    log = Log()
    for file_pattern in file_patterns:
        match = re.search(data_patterns[file_pattern], line)
        if match is None:
            continue
        if file_pattern == '%h':
            log.server_ip = match.group()
        elif file_pattern == '%t':
            date_str = match.group(1)
            date_object = datetime.strptime(date_str, '%d/%b/%Y:%H:%M:%S')
            formatted_date = date_object.strftime('%Y-%m-%d')
            log.date = formatted_date
        elif file_pattern == '%r':
            log.query = match.group(1)
        elif file_pattern == '%>s':
            log.response = match.group()
        elif file_pattern == '%b':
            log.weight = match.group()
    return log

def init_connection(db_info):
    '''Функция для инициализации подключения к БД'''
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_info['database'],
            user=db_info['user'],
            password=db_info['password'],
            host=db_info['host'],
            port=db_info['port'],
        )
    except Exception as e:
        print(f"Невозможно подключиться к БД: {e}")
    return connection

def pull_data(connection, logs):
    '''Функция для отправки информации на БД'''
    try:
        logs_records = ", ".join(["%s"] * len(logs))
        insert_query = (
            f"INSERT INTO logs (server_ip, date_time, log_query, response, weight) VALUES {logs_records}"
        )
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, logs)
        print('Данные отправлены успешно')
    except Exception as e:
        print(f'Некорректный запрос или ошибка сервера: {e}')
