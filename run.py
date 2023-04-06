import pandas as pd
from sqlalchemy import create_engine, text as sql_text
from loguru import logger
from crud.crud import get_yaml, fillPostgreSQL

csv_dir = './src/data.csv'
table_name = 'test_drive'
params = get_yaml('./params/params.yaml')['connection']


if __name__ == '__main__':
    df = pd.read_csv(csv_dir)
    logger.info('Wrote the .csv into DataFrame.')
    
    engine = create_engine(f"postgresql://{params['user']}:{params['password']}@{params['host']}:{5432}/{params['database']}")
    conn_sqlalchemy = engine.connect()
    logger.info(f"Connected to {params['database']}")

    fillPostgreSQL(table_name=table_name, df=df, engine=conn_sqlalchemy)