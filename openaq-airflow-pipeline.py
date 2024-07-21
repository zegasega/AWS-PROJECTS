from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def categorize_air_quality(value):
    if value <= 50:
        return "Good"
    elif 50 < value <= 100:
        return "Moderate"
    else:
        return "Bad"

def fetch_data():
    url = "https://api.openaq.org/v2/measurements?limit=3"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return data['results']
    else:
        return None

def process_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data_task')
    data_list = []
    for result in data:
        data_dict = {
            "Country": result['country'],
            "Location": result['location'],
            "Date (UTC)": result['date']['utc'],
            "Latitude": result['coordinates']['latitude'],
            "Longitude": result['coordinates']['longitude'],
            "Value": result['value']
        }
        data_list.append(data_dict)

    df = pd.DataFrame(data_list)
    df['Air_Quality'] = df['Value'].apply(categorize_air_quality)
    print(df)
    return df

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='process_data_task')
    if data is None:
        print("No data received from process_data_task")
        return None
    
    csv_data = data.to_csv('data.csv')
    return csv_data


def load_data(**kwargs):
    ti = kwargs.get('ti')
    if ti is None:
        raise ValueError("Parameter 'ti' is missing in kwargs.")
    
    data = ti.xcom_pull(task_ids='transform_data_task')
    if data is None or len(data) == 0:
        print("No data found in XCom. Skipping data loading.")
        return
    
    base_dir = os.path.expanduser('~')
    csv_path = os.path.join(base_dir, 'data.csv')
    data.to_csv(csv_path)

with DAG('openaq_data_pipeline_v2', 
         default_args=default_args,
         schedule_interval=timedelta(hours=1),
         catchup=False) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data
    )
    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data
    )
    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data
    )
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data
    )

    fetch_data_task >> process_data_task >> transform_data_task >> load_data_task
