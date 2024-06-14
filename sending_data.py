import config
import schedule
import data_functions

def main():
    # Инициализация подключения к базе данных
    connection = data_functions.init_connection(config.db_info)

    # Чтение логов из файлов
    logs = data_functions.read_data(config.file_paths, data_functions.data_patterns)
    logs = list(map(tuple, logs))  # Преобразование логов в формат кортежей

    # Выполнение отправки информации в базу данных
    # data_functions.pull_data(connection, logs) # Строка для отправки информации

if __name__ == '__main__':
    # Запуск основной программы по расписанию
    schedule.every(5).seconds.do(main)  # Вызываем функцию main каждые 5 секунд

    try:
        while True:
            schedule.run_pending()  # Проверяем и выполняем запланированные задачи
    except KeyboardInterrupt:
        print('Сборщик прекратил работу')  # Обработка прерывания пользователем
    except Exception as e:
        print(f'Возникла неприятность: {e}')  # Обработка других исключений
