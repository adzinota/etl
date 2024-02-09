# -------------------------------------------------------------------------------------------
# ==== MAIN =================================================================================
# -------------------------------------------------------------------------------------------

# Импортируем необходимые библиотеки
import pandas as pd
import yaml
from datetime import datetime

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.decorators import task

# Зададим константы путей к папкам и список имен файлов, создадим подключение к базе данных
IMPORT_PATH = '/home/joann/etl'

EXPORT_PATH = '/home/joann/etl/tmp'

CONNECTION = create_engine(PostgresHook(postgres_conn_id = 'etl').get_uri())

# Определим вспомогательные функции для работы с данными

# Базовая трансформация для всех файлов
def df_transform(name):
    df = pd.read_csv(f'{EXPORT_PATH}/{name}.csv', keep_default_na=False)

    df.drop(df.columns[0], axis=1, inplace=True)
    
    for column in df.columns:
        if 'DATE' in column:
            try:
                df[column] = pd.to_datetime(df[column], dayfirst=True)
            except:
                df[column] = pd.to_datetime(df[column])
    
    return df

# Запрос для логов (начало и конец процесса) 
def query_logs_tables(action_type, table_name):

    return f'''
        INSERT INTO logs.logs_etl
        VALUES ('{table_name}', '{datetime.now()}', '{action_type}')
        '''

# -------------------------------------------------------------------------------------------
# ==== ETL ==================================================================================
# -------------------------------------------------------------------------------------------

# Извлечение исходных данных из файлов и сохранение во временную папку
@task(task_id='extract')
def extract(name):

    path = f'{IMPORT_PATH}/{name}.csv'
    
    try:
        df = pd.read_csv(path, sep=';', keep_default_na=False)
    except:
        df = pd.read_csv(path, sep=';', keep_default_na=False, encoding='CP866')
    
    path = f'{EXPORT_PATH}/{name}.csv'
    df.to_csv(path, index=False, encoding='UTF-8')

# Трансформация файлов и сохранение во временную папку
@task(task_id='transform')
def transform(name):

    df = df_transform(name)

    if name == 'ft_posting_f':
        columns = ['OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK']
        df = df.groupby(columns, as_index=False).sum()
    elif name == 'md_exchange_rate_d':
        df = df.drop_duplicates()

    path = f'{EXPORT_PATH}/{name}.csv'
    df.to_csv(path, index=False, encoding='UTF-8')

# Загрузка обработанных датафреймов в базу данных
@task(task_id='load')
def load(name):
    metadata_obj = MetaData(schema = 'ds')
    table = Table(name, metadata_obj, autoload_with=CONNECTION)
    
    df = pd.read_csv(f'{EXPORT_PATH}/{name}.csv', keep_default_na=False)

    insert_statement = insert(table).values(df.values.tolist())
    upsert_statement = insert_statement.on_conflict_do_update(constraint=table.primary_key, set_=dict(insert_statement.excluded))
    CONNECTION.execute(upsert_statement)

# -------------------------------------------------------------------------------------------
# ==== DAG ==================================================================================
# -------------------------------------------------------------------------------------------

with open('/home/joann/airflow/dags/dag_config.yaml', 'r') as file:
    dags = yaml.safe_load(file)

for dag_id, config in dags.items():

    with DAG(f'{config["dag_name"]}',
         start_date=datetime(2024,1,1),
         schedule_interval=None,
         catchup=False) as dag_name:

        # Таск sleep
        sleep_5s = BashOperator(task_id='sleep_5s',
                            dag=dag_name,
                            bash_command='sleep 5s')
        
        # Таск start
        @task(task_id=f'start')
        def start(name, **kwargs):
            CONNECTION.execute(query_logs_tables('start', name))

        # Таск finish
        @task(task_id=f'finish')
        def finish(name, **kwargs):
            CONNECTION.execute(query_logs_tables('finish', name))
            
        name = config['file_name']

        start(name) >> sleep_5s >> extract(name) >> transform(name) >> load(name) >> finish(name)