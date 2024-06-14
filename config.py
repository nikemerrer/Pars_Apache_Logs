# config.py

# Пути к лог-файлам
file_paths = [
    (r'C:\Users\sssas\Desktop\Проект\Apache-log-parser\.venv\Scripts\logs1', '%h,%t,%r,%>s,%b'),
    (r'C:\Users\sssas\Desktop\Проект\Apache-log-parser\.venv\Scripts\logs2', '%r,%>s')
]

# Информация для подключения к базе данных PostgreSQL
db_info = {
    'database': 'log_analyzer',
    'user': 'postgres',
    'password': '31415',
    'host': '127.0.0.1',
    'port': '5432'
}
