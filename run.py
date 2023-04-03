import pandas as pd
import psycopg2
from loguru import logger
import sys
from crud.crud import get_yaml, fillPostgreSQL

def load_args():
    try:
        csv_dir, table_name = sys.argv[1:4]
        return csv_dir, table_name
    except ValueError:
        logger.error('Incorrect number of args.')


if __name__ == '__main__':
    csv_dir, table_name = load_args()
    params = get_yaml('./params/params.yaml')['connection']

    df = pd.read_csv(csv_dir)
    logger.info('Wrote the .csv into DataFrame.')
    
    df = df.drop(['url'], axis=1)  # Убрал столбец 'url'
    df = df.dropna(subset=['streams'])  # Убрал все NaN'ы из col streams
    df['title'] = df['title'].fillna('Unknown title')  # Заполнил все пропущенные значения в col title
    df = df.reset_index(drop=True)  # Сбросил индекс
    logger.info('Got rid of nans')
    
    categ_cols = df.select_dtypes(include=['object']).columns
    df[categ_cols] = df[categ_cols].apply(lambda x: x.str.replace("'", ''))

    conn = psycopg2.connect(
        host=params['host'],
        database=params['database'],
        user=params['user'],
        password=params['password']
        )
    logger.info(f"Connected to {params['database']}")

    fillPostgreSQL(table_name=table_name, df=df, conn=conn)
    
    # run.sh "D:\repos\smallbigdata\src\charts.csv" "spotify_charts"