from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from sqlalchemy import create_engine
from pandas import json_normalize
from sqlalchemy import create_engine
import requests
import json


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 30),
    'max_active_runs': 1
}

def minsoc_young_families():
    
    url = "???"

    response = requests.get(url)
    
    data = response.json()

    df = pd.DataFrame(data)

    normalized_data = json_normalize(df['infoByTerritory'])

    df = pd.concat([df, normalized_data], axis=1)

    df = df.drop('infoByTerritory', axis=1)

    engine = create_engine('postgresql+psycopg2://???')

    df.to_sql('minsoc_young_families', engine,schema='raw', if_exists='append', index = 0)

with DAG('minsoc_young_families',
         default_args=default_args, 
         schedule_interval='15 12 * * *',
         catchup = False,
         tags = ['minsoc']
        ) as dag:
    
    
    t1 = PythonOperator(
    task_id='minsoc_young_families',
    python_callable=minsoc_young_families,
    dag=dag,
)
    