import config
import os
import re
import psycopg2
from data_functions import read_data, data_patterns, init_connection, pull_data
import datetime

def create_query(answer:[str]) -> str:
    '''Функция для динамического формирования SQL запроса'''
    answer = answer.split(' ')  # Разбиваем введенный текст на слова
    date_pattern = re.compile(r'\d{4}(-\d{2}){2}')  # Паттерн для даты YYYY-MM-DD
    dates = []
    columns = ''
    trash_mods = []  # Список для хранения неопознанных команд

    # Проходим по каждому слову из введенной команды
    for mod in answer:
        if date_pattern.match(mod) and len(dates) < 2:
            dates.append(mod)  # Если слово соответствует паттерну даты, добавляем в список дат
        elif mod in ['log_ip', 'server_ip', 'date_time', 'log_query', 'response', 'weight'] and mod not in columns:
            # Если слово соответствует столбцу базы данных и этот столбец еще не добавлен, добавляем его
            if columns == '':
                columns += mod
            else:
                columns += ', ' + mod
        elif mod != 'select_logs':
            trash_mods.append(mod)  # Все остальные слова добавляем в список неопознанных команд

    if columns == '':
        columns = '* '  # Если не указаны столбцы, выбираем все

    query = 'select ' + columns + ' from logs'  # Начало формирования SQL запроса
    where_query = ''

    if len(dates) == 1:
        # Если передана одна дата, формируем условие WHERE от этой даты до текущей
        date1 = list(map(int, dates[0].split('-')))
        where_query = f' where date_time > TO_DATE(\'{date1[0]}/{date1[1]}/{date1[2]}\', \'YYYY/MM/DD\') and date_time < CURRENT_DATE'
    elif len(dates) == 2:
        # Если переданы две даты, выбираем данные между ними
        date1 = list(map(int, dates[0].split('-')))
        date2 = list(map(int, dates[1].split('-')))
        # Создаем условие в зависимости от наибольшей из дат
        if datetime.date(date1[0], date1[1], date1[2]) < datetime.date(date2[0], date2[1], date2[2]):
            where_query = f' where date_time > TO_DATE(\'{date1[0]}/{date1[1]}/{date1[2]}\', \'YYYY/MM/DD\') and date_time < TO_DATE(\'{date2[0]}/{date2[1]}/{date2[2]}\', \'YYYY/MM/DD\')'
        else:
            where_query = f' where date_time > TO_DATE(\'{date2[0]}/{date2[1]}/{date2[2]}\', \'YYYY/MM/DD\') and date_time < TO_DATE(\'{date1[0]}/{date1[1]}/{date1[2]}\', \'YYYY/MM/DD\')'

    query += where_query  # Добавляем условие WHERE к запросу
    query += ';'  # Завершаем запрос

    if len(trash_mods) > 0:
        print(f'неопознанные команды: {trash_mods}')  # Выводим неопознанные команды, если они есть

    return query, columns  # Возвращаем сформированный запрос и список столбцов


def fetch_data_from_db(answer, connection) -> list[dict[str, str]]:
    '''Функция для выполнения запроса к базе данных'''
    query, columns = create_query(answer)  # Формируем SQL запрос
    cursor = connection.cursor()  # Создаем курсор для выполнения запроса
    cursor.execute(query)  # Выполняем запрос
    result = cursor.fetchall()  # Получаем результаты запроса
    json_result = []  # Список для хранения результатов в формате JSON

    # Если выбраны все столбцы, определяем их порядок
    if columns.strip() == '*':
        columns = ['log_ip', 'server_ip', 'date_time', 'log_query', 'response', 'weight']
    else:
        columns = [col.strip() for col in columns.split(',')]

    for log in result:
        json_format = {}  # Словарь для хранения данных одной записи в формате JSON
        # Проверяем, что количество столбцов соответствует количеству элементов в записи
        if len(log) != len(columns):
            print(columns, log)
            raise ValueError("Количество столбцов и элементов в результате не совпадает")
        for i, column in enumerate(columns):
            # Создаем словарь в формате 'столбец':'значение'
            json_format[column] = str(log[i])
        json_result.append(json_format)  # Добавляем запись в список результатов

    return json_result


try:
    connection = init_connection(config.db_info)  # Инициализация подключения к базе данных
    if connection is None:
        raise Exception("Не удалось установить соединение с базой данных")
    while True:
        answer = input('>>> ')  # Ожидание ввода команды от пользователя
        if 'export' in answer:
            # Инициализация экспорта данных в базу данных
            logs = read_data(config.file_paths, data_patterns)
            logs = list(map(tuple, logs))
            pull_data(connection, logs)
        elif 'select' in answer:
            # Выполнение запроса на выборку данных из базы данных
            logs = fetch_data_from_db(answer, connection)
            for log in logs:
                print(log)  # Вывод результатов выборки
        else:
            print('Неопознанная команда')
except KeyboardInterrupt:
    print('Сессия завершена')  # Обработка прерывания пользователем
