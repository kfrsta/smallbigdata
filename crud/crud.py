import pandas as pd
import psycopg2
import yaml
from typing import List
from loguru import logger
from tqdm import tqdm

def get_yaml(path: str) -> dict:
    with open(path) as f:
        params = yaml.load(f, Loader=yaml.FullLoader)
    
    return params


## Возвращает список типов данных для таблицы SQL
def get_sql_dtypes(df: pd.DataFrame) -> List[str]:
    list_of_types = []
    dtypes = {
        'int64': 'INT',
        'object': 'VARCHAR',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN'
        }

    for i in df.dtypes:
        list_of_types.append(dtypes[f'{i}'])
    
    return list_of_types


## Фильтр форматирования названий колонок
def mult_replace(column: str) -> str:
    ban = [',', '.', '(', ')', '-', '+', '~', ' ', '/']
    for i in ban:
        column = column.replace(i, '')
    return column


## Форматирование названий колонок
def edit_columns(columns: List[str]) -> List[str]:
    li = []
    for i, column in enumerate(columns):
        li.append(mult_replace(column))

    return li


## Ввести ряд
def insert_row(df, idx, table_name: str) -> str:
    sql = f'INSERT INTO {table_name}('
    columns = edit_columns(df.columns)
    list_of_types = get_sql_dtypes(df=df)

    for i in columns:
        sql += f'{i}, '
    sql = sql.rstrip(', ') + ') VALUES ('

    for i in range(df.shape[1]):
        if list_of_types[i] == 'VARCHAR':
            sql += f"'{df[df.columns[i]][idx]}', "
        else:
            sql += f"{df[df.columns[i]][idx]}, "

    sql = sql.rstrip(', ') + ');'

    return sql


## Вести целый .csv
def insert_df(df: pd.DataFrame, conn: psycopg2.connect, table_name: str) -> None:
    logger.info('Transfering the DataFrame into the database.')

    with conn: 
        cur = conn.cursor()

        for idx in tqdm(range(df.shape[0]), desc='Transfer'):
            sql = insert_row(df=df, idx=idx, table_name=table_name)
            cur.execute(sql)


def create_table_request(table_name, df) -> str:
    sql = f'CREATE TABLE {table_name} ('
    columns = edit_columns(df.columns)
    list_of_types = get_sql_dtypes(df=df)

    for i in range(df.shape[1]):
        sql += f"{columns[i]} {list_of_types[i]} NOT NULL, "

    return sql.rstrip(', ') + ');'  # Готовый запрос на создание таблицы


## Создать запрос на создание таблицы
def fillPostgreSQL(df: pd.DataFrame, conn: psycopg2.connect, table_name: str = 'test_table') -> None:
    sql = create_table_request(table_name=table_name, df=df)
    
    with conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
        except psycopg2.errors.DuplicateTable:
            logger.warning(f'Table "{table_name}" already exists.')
            return

    insert_df(df, conn, table_name)
    logger.success(f'.csv {table_name} has been successfully loaded!')