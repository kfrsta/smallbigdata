from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
import psycopg2
from loguru import logger
import joblib

from airflow.decorators import dag, task

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
    schedule='@weekly',
    start_date=datetime(2023, 4, 10),
    catchup=False,
    tags=["fine_tuning"],
)
def fine_tuning_model_api():   
    @task()
    def tune_model():
        data = pd.read_csv("./model/test_tuning.csv",delimiter=";")
        data = transform_data(data.copy())
        
        X = data.drop('streams', axis=1)
        y = data['streams']
        
        model = joblib.load("model/model.pkl")
        model.fit(X, y)
        
        joblib.dump(model, './model/model_ft.pkl')    
        file = open("model/log_tuning.txt", mode = "a")
        
        data_log = datetime.now()
        file.write(f"Date: {data_log.strftime('%Y-%m-%d')} Status: fitted.")
        logger.success('Модель дообучена на новых данных!')
    
    tune_model()
    
fine_tuning_model_api()
