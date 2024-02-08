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

# Выбор трансформации в зависимости от имени файла
def select_transform(name_file):
    tasks_for_select = {'ft_balance_f': transform_0,
                        'ft_posting_f': transform_1,
                        'md_account_d': transform_0,
                        'md_currency_d': transform_0,
                        'md_exchange_rate_d': transform_2,
                        'md_ledger_account_s': transform_0}
    return tasks_for_select[name_file]

# Запрос для логов (начало и конец процесса) 
def query_logs_tables(type_log, tname):

    return f'''
        INSERT INTO logs.logs_etl
        VALUES ('{tname}', '{datetime.now()}',
        '{type_log}')
        '''

# -------------------------------------------------------------------------------------------
# ==== ETL ==================================================================================
# -------------------------------------------------------------------------------------------

# Извлечение исходных данных из файлов и сохранение во временную папку
@task(task_id='extract')
def extract(name_file_csv):

    path = f'{IMPORT_PATH}/{name_file_csv}.csv'
    
    try:
        df = pd.read_csv(path, sep=';', keep_default_na=False)
    except:
        df = pd.read_csv(path, sep=';', keep_default_na=False, encoding='CP866')
    
    path = f'{EXPORT_PATH}/{name_file_csv}.csv'
    df.to_csv(path, index=False, encoding='UTF-8')

# Основная трансформация для тех файлов, где не требуется дополнительная обработка
@task(task_id='transform_0')
def transform_0(name_df):

    pd_df = df_transform(name_df)

    path = f'{EXPORT_PATH}/{name_df}.csv'
    pd_df.to_csv(path, index=False, encoding='UTF-8')

# Трансформация для ft_posting_f.csv с группировкой по полям
@task(task_id='transform_1')
def transform_1(name_df):

    pd_df = df_transform(name_df)

    columns = ['OPER_DATE', 'CREDIT_ACCOUNT_RK', 'DEBET_ACCOUNT_RK']

    pd_df = pd_df.groupby(columns, as_index=False).sum()

    path = f'{EXPORT_PATH}/{name_df}.csv'
    pd_df.to_csv(path, index=False, encoding='UTF-8')

# Трансформация для md_exchange_rate_d.csv с удалением дубликатов
@task(task_id='transform_2')
def transform_2(name_df):

    pd_df = df_transform(name_df)

    pd_df = pd_df.drop_duplicates()

    path = f'{EXPORT_PATH}/{name_df}.csv'
    pd_df.to_csv(path, index=False, encoding='UTF-8')

# Загрузка обработанных датафреймов в базу данных
@task(task_id='load')
def load(table_name):
    metadata_obj = MetaData(schema = 'ds')
    table = Table(table_name, metadata_obj, autoload_with=CONNECTION)
    
    df = pd.read_csv(f'{EXPORT_PATH}/{table_name}.csv', keep_default_na=False)

    insert_statement = insert(table).values(df.values.tolist())
    upsert_statement = insert_statement.on_conflict_do_update(constraint=table.primary_key, set_=dict(insert_statement.excluded))
    CONNECTION.execute(upsert_statement)

# -------------------------------------------------------------------------------------------
# ==== DAG ==================================================================================
# -------------------------------------------------------------------------------------------

with open('/home/joann/airflow/dags/dag_config.yaml', 'r') as file:
    dags = yaml.safe_load(file)

for dag_id, config in dags.items():

    with DAG(f'{config["dag_id"]}',
         start_date=datetime(2024,1,1),
         schedule_interval=None,
         catchup=False) as dag_id:

        # Таск sleep
        sleep_5s = BashOperator(task_id='sleep_5s',
                            dag=dag_id,
                            bash_command='sleep 5s')
        
        # Таск start
        @task(task_id=f'start')
        def start(table_for_query, **kwargs):
            CONNECTION.execute(query_logs_tables('start', table_for_query))

        # Таск finish
        @task(task_id=f'finish')
        def finish(table_for_query, **kwargs):
            CONNECTION.execute(query_logs_tables('finish', table_for_query))
            
        filename = config['filename']

        start(filename) >> sleep_5s >> extract(filename) >> select_transform(filename)(filename) >> load(filename) >> finish(filename)