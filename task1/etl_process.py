# -------------------------------------------------------------------------------------------
# ==== MAIN =================================================================================
# -------------------------------------------------------------------------------------------

# Импортируем необходимые библиотеки
import pandas as pd
from datetime import datetime

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group

# Зададим константы путей к папкам и список имен файлов, создадим подключение к базе данных
IMPORT_PATH = '/home/joann/etl'

EXPORT_PATH = '/home/joann/etl/tmp'

FILENAMES = ['ft_balance_f', 'ft_posting_f','md_account_d', 'md_currency_d', 'md_exchange_rate_d', 'md_ledger_account_s']

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

# Запрос для общих логов (начало и конец процесса) 
def query_logs_total(time_log, type_log):
        return f'''
            INSERT INTO logs.logs_total
            (action_date, action_type)
            VALUES ('{time_log}', '{type_log}'
        )'''

# Запрос для логов по каждой таблице (начало и конец процесса) 
def query_logs_tables(type_log, ti, tname):
    pk_log = ti.xcom_pull(task_ids='start', key='tstart')
    return f'''
        INSERT INTO logs.logs_tables
        VALUES ('{tname}', '{datetime.now()}',
        '{type_log}', {pk_log})
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

with DAG('etl_process',
         start_date=datetime(2024, 1 ,1),
         schedule_interval=None,
         catchup=False) as dag:

    # Таск sleep
    sleep_5s = BashOperator(task_id='sleep_5s',
                            dag=dag,
                            bash_command='sleep 5s')
    
    # Таск start
    @task(task_id='start')
    def start(**kwargs):
        ti = kwargs['ti']
        time_start_process = datetime.now()
        CONNECTION.execute(query_logs_total(time_start_process, 'start'))
        pk_log = CONNECTION.execute(f'''
            SELECT l.log_id
            FROM logs.logs_total as l
            ORDER BY l.log_id DESC 
            LIMIT 1 
        ''').fetchone()[0]
        ti.xcom_push(value=pk_log, key='tstart')
    
    # Таск finish
    @task(task_id='finish')
    def finish():
        CONNECTION.execute(query_logs_total(datetime.now(), 'finish'))

    # Группа тасков для всех таблиц
    groups = []
    for group_name in FILENAMES:

        etl_id = f'etl_{group_name}'
        log_id = f'log_{group_name}'

        # Таск etl_log_start
        @task(task_id=f'etl_log_start')
        def etl_log_start(table_for_query, **kwargs):
            CONNECTION.execute(query_logs_tables('start', kwargs['ti'], table_for_query))

        # Таск etl_log_finish
        @task(task_id=f'etl_log_finish')
        def etl_log_finish(table_for_query, **kwargs):
            CONNECTION.execute(query_logs_tables('finish', kwargs['ti'], table_for_query))

        # Группа тасков ETL для каждой таблицы
        @task_group(group_id=etl_id)
        def etl_process():
            extract(name_file_csv=group_name) >> select_transform(group_name)(group_name) >> load(group_name)


        # Группа тасков ETL с логами
        #@task_group(group_id=log_id)
        @task_group(group_id=log_id)
        def log_group_exe(**kwargs):
            etl_log_start(group_name) >> etl_process() >> etl_log_finish(group_name)

        groups.append(log_group_exe())

    start() >> sleep_5s >> groups >> finish()