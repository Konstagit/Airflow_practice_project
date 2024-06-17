import os
import sys
import requests
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import tqdm

# Define constants
SHARED_DIR = '/opt/airflow/shared'
DATA_FILE = 'profit_table.csv'
RESULT_FILE = 'flags_activity.csv'
PRODUCTS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

# Добавляем путь к директории, где находится transform_script.py
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from transform_script import transform  # импортируем функцию transfrom из скрипта

# Настройки по умолчанию для DAG
DAG_ID = 'konstantinov_DAG_02'  # имя DAG
default_args = {
    'owner': 'konstantinov',  # владелец DAG
    'start_date': pendulum.datetime(2023, 4, 5, tz=pendulum.timezone("Europe/Moscow")),  # дата начала выполнения DAG
    'schedule_interval': '0 0 5 * *',  # расписание запуска DAG (каждое 5-е число месяца в 00:00)
    'retries': 3,  # количество попыток перезапуска задач в случае ошибки
    "retry_delay": timedelta(seconds=60),  # время задержки между перезапусками
    'description': 'parallel',  # описание DAG
    'max_active_runs': 1,  # максимальное количество активных запусков DAG
    'catchup': False,  # отключение догоняющего выполнения
    'shared_dir': SHARED_DIR,
    'data_file': DATA_FILE,
    'result_file': RESULT_FILE,  
    'products': PRODUCTS
}

# Шаг 1: Функция для скачивания данных
def DownloadData(date, **kwargs):
    """
    Функция для скачивания и сохранения данных из csv-файла.
    :param date: дата в формате 'YYYY-MM-DD'
    """
    url = 'https://drive.google.com/uc?export=download&id=1ZGYlpWXEIUpZPtULLCom3i6-ZXe7dIiJ'  # URL для загрузки данных
    data_dir = os.path.join(default_args['shared_dir'], 'tmp', 'airflow', 'data')  # временная директория для сохранения данных
    os.makedirs(data_dir, exist_ok=True)  # создаем директорию, если она не существует
    output_path = os.path.join(data_dir, f"{default_args['data_file']}_{date}.csv")  # путь для сохранения файла

    response = requests.get(url)  # загружаем данные по указанному URL
    response.raise_for_status()  # проверка успешности запроса

    with open(output_path, 'wb') as file:  # открываем файл для записи в бинарном режиме
        file.write(response.content)  # записываем содержимое ответа в файл

    # Логирование успешного скачивания данных
    ti = kwargs['ti']  # Получаем экземпляр TaskInstance из kwargs
    ti.xcom_push(key='download_success', value=True)  # Сохраняем значение True в XCom
    print(f"Файл успешно загружен и сохранен в {output_path}")

# Шаг 2: Функция для обработки данных
def ProcessProduct(date, shared_dir, data_file, products, **kwargs):
    """
    Функция для обработки данных по каждому продукту.
    :param date: дата в формате 'YYYY-MM-DD'
    :param shared_dir: директория для хранения данных
    :param data_file: имя файла с данными
    :param products: список продуктов
    """
    data_path = os.path.join(shared_dir, 'tmp', 'airflow', 'data', f"{data_file}_{date}.csv")  # путь к файлу данных
    df = pd.read_csv(data_path)  # читаем данные из csv-файла

    # Обрабатываем только 5% от загруженного файла
    sample_size = int(len(df) * 0.05)
    df = df.sample(n=sample_size, random_state=42)

    all_flags = []
    for product in products:
        transformed_df = transform(df, date, product)  # трансформируем данные с использованием функции `transform`
        all_flags.append(transformed_df)

    final_df = pd.concat(all_flags, axis=1).T.drop_duplicates().T  # объединяем все флаги в один DataFrame
    Load(final_df, date, **kwargs)

# Шаг 3: Функция для загрузки данных
def Load(df, date, **kwargs):
    """
    Функция для загрузки данных в csv-файл.
    :param df: DataFrame с трансформированными данными
    :param date: дата в формате 'YYYY-MM-DD'
    """
    base_dir = os.path.join(default_args['shared_dir'], 'tmp', 'airflow', 'data')  # директория для хранения результатов
    output_path = os.path.join(base_dir, f"{default_args['result_file']}.csv")  # путь для сохранения файла

    os.makedirs(base_dir, exist_ok=True)  # создаем директорию, если она не существует

    if os.path.exists(output_path):
        df.to_csv(output_path, mode='w', index=False, header=False)  # перезаписываем файл, если он уже существует
    else:
        df.to_csv(output_path, index=False)  # сохраняем новые данные в новый файл, если файл не существует

with DAG(
    DAG_ID,
    default_args=default_args,
    description=default_args.get("description"),
    start_date=default_args.get("start_date"),
    schedule_interval=default_args.get("schedule_interval"),
    catchup=default_args.get("catchup"),
    max_active_runs=default_args.get("max_active_runs")
) as dag:
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=DownloadData,
        op_kwargs={'date': '{{ ds }}'},
        provide_context=True
    )

    product_tasks = []
    for product in default_args['products']:
        task = PythonOperator(
            task_id=f'process_{product}', 
            python_callable=ProcessProduct,
            op_kwargs={'date': '{{ ds }}', 'shared_dir': default_args['shared_dir'], 'data_file': default_args['data_file'], 'products': [product]},
            provide_context=True
        )
        product_tasks.append(task)
        download_task >> task  # задаем порядок выполнения задач (сначала download_task, затем каждая задача по продукту)