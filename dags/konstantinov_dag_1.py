import os 
import sys
import requests
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import tqdm
from transform_script import transform

# Define constants
SHARED_DIR = '/opt/airflow/shared'
DATA_FILE = 'profit_table.csv'
RESULT_FILE = 'flags_activity.csv'
PRODUCTS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

# Добавляем путь к директории, где находится transform_script.py
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from transform_script import transform  # импортируем функцию transfrom из скрипта

# Настройки по умолчанию для DAG
DAG_ID = 'konstantinov_DAG_01'  # имя DAG
default_args = {
    'owner': 'konstantinov',  # владелец DAG
    'start_date': pendulum.datetime(2023, 4, 5, tz=pendulum.timezone("Europe/Moscow")),  # дата начала выполнения DAG
    'schedule_interval': '0 0 5 * *',  # расписание запуска DAG (каждое 5-е число месяца в 00:00)
    'retries':3,  # количество попыток перезапуска задач в случае ошибки
    "retry_delay": timedelta(seconds=60),  # время задержки между перезапусками
    'description': 'single',  # описание DAG
    'max_active_runs': 1,  # максимальное количество активных запусков DAG
    'catchup': False,  # отключение догоняющего выполнения
}
# Шаг 1: Функция для скачивания данных
def DownloadData(date, **kwargs):
    """
    Функция для скачивания и сохранения данных из csv-файла.
    :param date: дата в формате 'YYYY-MM-DD'
    """
    url = 'https://drive.google.com/uc?export=download&id=1ZGYlpWXEIUpZPtULLCom3i6-ZXe7dIiJ'  # URL для загрузки данных
    data_dir = 'shared/tmp/airflow/data/'  # временная директория для сохранения данных
    os.makedirs(data_dir, exist_ok=True)  # создаем директорию, если она не существует
    output_path = os.path.join(data_dir, f'profit_table_{date}.csv')  # путь для сохранения файла

    response = requests.get(url)  # загружаем данные по указанному URL
    response.raise_for_status()  # проверка успешности запроса

    with open(output_path, 'wb') as file:  # открываем файл для записи в бинарном режиме
        file.write(response.content)  # записываем содержимое ответа в файл

    # Логирование успешного скачивания данных
    ti = kwargs['ti'] # Получаем экземпляр TaskInstance из kwargs
    ti.xcom_push(key='download_success', value=True) # Сохраняем значение True в XCom
    print(f"Файл успешно загружен и сохранен в {output_path}")

# Шаг 2: Фцнкция для обработки данных
def ProcessProduct(date, **kwargs):
    """
    Функция для обработки данных по каждому продукту.
    :param product: название продукта (буква от 'a' до 'j')
    :param date: дата в формате 'YYYY-MM-DD'
    """
    data_path = f'shared/tmp/airflow/data/profit_table_{date}.csv'  # путь к файлу данных
    df = pd.read_csv(data_path)  # читаем данные из csv-файла
    product_list = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']  # список продуктов
    all_flags = []

    for product in product_list:
        transformed_df = transform(df, date, product)  # трансформируем данные с использованием функции `transform`
        all_flags.append(transformed_df)

    final_df = pd.concat(all_flags, axis=1).T.drop_duplicates().T  # объединяем все флаги в один DataFrame
    Load(final_df, date)  # загружаем трансформированные данные

# Шаг 3: Функция для загрузки данных
def Load(df, date):
    """
    Функция для загрузки данных в csv-файл.
    :param df: DataFrame с трансформированными данными
    :param date: дата в формате 'YYYY-MM-DD'
    """
    base_dir = 'shared/tmp/airflow/data/'  # временная директория для сохранения файла
    output_path = os.path.join(base_dir, 'flags_activity.csv')  # путь для сохранения итогового файла

    if os.path.exists(output_path):
        df.to_csv(output_path, mode='a', index=False, header=False)  # дописываем данные в существующий файл
    else:
        df.to_csv(output_path, index=False)  # сохраняем новые данные в новый файл, если файл не существует

# Определяем DAG
with DAG(
    DAG_ID,  # имя DAG
    default_args=default_args,  # аргументы по умолчанию
    description=default_args.get("description"),
    start_date=default_args.get("start_date"),
    schedule_interval=default_args.get("schedule_interval"),
    catchup=default_args.get("catchup"),
    max_active_runs=default_args.get("max_active_runs")
) as dag:

    # Задача для загрузки данных
    download_task = PythonOperator(
        task_id='download_data',  # идентификатор задачи
        python_callable=DownloadData,  # вызываемая функция
        op_kwargs={'date': '{{ ds }}'},  # аргументы функции (дата запуска DAG)
        provide_context=True  # включаем контекст
    )

    # Задача для трансформации данных
    transform_task = PythonOperator(
        task_id='transform',  # уникальный идентификатор задачи для трансформации данных
        python_callable=ProcessProduct,  # функция, которая будет вызвана для выполнения задачи трансформации данных
        op_kwargs={'date': '{{ ds }}'},  # передаем аргумент 'date' в функцию, который будет заменен на дату запуска DAG (Execution Date)
        provide_context=True  # включаем контекст
    )

    download_task >> transform_task  # задаем порядок выполнения задач