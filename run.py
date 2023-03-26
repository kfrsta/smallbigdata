import pandas as pd
import psycopg2
from loguru import logger
import sys
from crud.crud import get_yaml, fillPostgreSQL

def load_args():
    try:
        csv_dir, table_name, var = sys.argv[1:4]
        return csv_dir, table_name, var
    except ValueError:
        logger.error('Incorrect number of args.')


if __name__ == '__main__':
    csv_dir, table_name, var = load_args()
    params = get_yaml('./params/params.yaml')

    df = pd.read_csv(csv_dir)
    logger.info('Wrote the .csv into DataFrame.')
    
    df = df.drop(['url'], axis=1)
    df = df.dropna(subset=['streams'])
    df['title'] = df['title'].fillna('Unknown title')
    df = df.reset_index(drop=True)
    logger.info('Got rid of nans')
    
    categ_cols = df.select_dtypes(include=['object']).columns
    df[categ_cols] = df[categ_cols].apply(lambda x: x.str.replace("'", ''))

    conn = psycopg2.connect(
        host=params['connection']['host'],
        database=params['connection']['database'],
        user=params['connection']['user'],
        password=params['connection']['password']
        )
    logger.info(f"Connected to {params['connection']['database']}")

    fillPostgreSQL(table_name=table_name, df=df, conn=conn)