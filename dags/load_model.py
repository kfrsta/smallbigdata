from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
import psycopg2
from sqlalchemy import create_engine, text as sql_text
import yaml
from loguru import logger
import joblib

from airflow.decorators import dag, task

def get_yaml(path: str) -> dict:
    with open(path) as f:
        params = yaml.load(f, Loader=yaml.FullLoader)
    
    return params

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df['date'] = df.date.values.astype(np.int64) // 10 ** 9
        
        encoder = LabelEncoder()
        features = list(df.select_dtypes(['object']).columns)

        for feature in features:
            df[feature] = encoder.fit_transform(df[feature])
            
        scaler = StandardScaler()
        nums = ["title", "date", "artist", "trend", "rank"]

        df[nums] = scaler.fit_transform(df[nums])
        df = df.drop(['chart', 'title'], axis=1)
        
        return df

@dag(
    schedule='@daily',
    start_date=datetime(2023, 4, 8),
    catchup=False,
    tags=["prediction"]
)
def taskflow_model_api():   
    @task()
    def train_model():
        params = get_yaml('./params/params.yaml')['connection']
        engine = create_engine(f"postgresql://{params['user']}:{params['password']}@{params['host']}:{5432}/{params['database']}")
        conn = engine.connect()
        
        data = pd.read_csv("./src/test.csv",delimiter=";")
        X = transform_data(data.copy())
        
        model = joblib.load("model/model.pkl")
        data["predictions"] = model.predict(X)
        
        data_log = datetime.now().strftime('%Y-%m-%d')
        data["date_input"] = data_log
        
        data.to_csv('model/pred_log.txt', sep='\t', index=False)
        data.to_csv('./src/predict.csv', sep=';')
        data.to_sql(name='predictions', con=conn, if_exists='replace')
        
        logger.success('Table successfully loaded!')
        
        return data
    
    train_model()
taskflow_model_api()
