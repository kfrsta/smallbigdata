import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text as sql_text
import yaml
from typing import List
from loguru import logger
from tqdm import tqdm

def get_yaml(path: str) -> dict:
    """### Возвращает список параметров подключения из .yaml

    Args:
        path (str): Путь к .yaml

    Returns:
        dict: Словарь из параметров
    """
    with open(path) as f:
        params = yaml.load(f, Loader=yaml.FullLoader)
    
    return params


## Создать запрос на создание таблицы
def fillPostgreSQL(df: pd.DataFrame, engine, table_name: str = 'test_table') -> None:
    """### Описание

    Args:
        df (pd.DataFrame): Датафрейм для выгрузки в БД
        engine (_type_): sqlalchemy engine
        table_name (str, optional): Название создаваемой таблицы. Defaults to 'test_table'.
    """
    df = df.drop(['url'], axis=1)  # Убрал столбец 'url'
    df = df.dropna(subset=['streams'])  # Убрал все NaN'ы из col streams
    
    df['title'] = df['title'].fillna('Unknown title')  # Заполнил все пропущенные значения в col title
    df = df.reset_index(drop=True)  # Сбросил индекс
    logger.info('Got rid of nans')
    
    categ_cols = df.select_dtypes(include=['object']).columns
    df[categ_cols] = df[categ_cols].apply(lambda x: x.str.replace("'", ''))

    df.to_sql(table_name, engine)
    logger.success(f'.csv {table_name} has been successfully loaded!')