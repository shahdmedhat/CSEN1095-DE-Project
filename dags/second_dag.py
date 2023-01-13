from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

def write_df_to_csv_file(df, file_name):
    file_name = file_name + ".csv"
    df.to_csv(file_name, index = False)

def extract_transform_load(filename):
    if(os.path.exists("/opt/airflow/data/finaldf.csv")):
        return
    df = pd.read_csv(filename, index_col = 0, parse_dates = ['date'], na_values = ["Data missing or out of range", -1])
    write_df_to_csv_file(df, '/opt/airflow/data/finaldf')



with DAG(
    dag_id = 'uk_accidents_2010_etl_pipeline',
    schedule_interval = '@once',
    default_args = default_args,
    description = 'UK Accidents 2010 ETL Pipeline',
    tags = ['uk-accidents-2010-pipeline'],
) as dag:
    extract_transform_load_task = PythonOperator(
        task_id = 'extract_transform_load_task',
        python_callable = extract_transform_load,
        op_kwargs={
            "filename": '/opt/airflow/data/2010_Accidents_UK.csv'
        },
    )

