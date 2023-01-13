from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine
import os

from scripts.missingValues import handleMissingValues
from scripts.outliers import handleOutliers
from scripts.duplicates import handleDuplicates
from scripts.encoding import labelEncoding
from scripts.discretization import discretize
from scripts.normalization import normalize

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

def extract_transform_load(filename):
    if(os.path.exists("/opt/airflow/data/finaldf.csv")):
        return
    df = pd.read_csv(filename, index_col = 0,  parse_dates = ['date'], na_values= ["Data missing or out of range" , -1])
    
    handleMissingValues(df)
    handleOutliers(df)
    labelEncoding(df)
    discretize(df)
    normalize(df)
    
    file_name = "/opt/airflow/data/finaldf.csv"
    df.to_csv(file_name)

# def extract_figures(filename):

def load_to_postgres(filename, lookup_filename):
    df = pd.read_csv(filename)
    lookup = pd.read_csv(lookup_filename)
    
    engine = create_engine('postgresql://root:root@pgdatabase:5432/2010_Accidents_UK')
    
    try:
        df.to_sql('2010_Accidents_UK', con = engine, if_exists = 'replace', index = False)
        lookup.to_sql('lookup_table', con = engine, if_exists = 'replace', index = False)
    except:
        pass

def create_dashboard(filename):
    df = pd.read_csv(filename)
    
    fig = px.histogram(df, x = 'weekend', title = 'Number of accidents in weekends vs weekdays')
    fig.update_layout(
        xaxis_title = 'Weekend',
        yaxis_title = 'Accident Count'
    )
    
    fig2 = px.histogram(df, x = 'weather_conditions', y= 'number_of_casualties', title = 'Number of casualties per weather condition')
    fig2.update_layout(
        xaxis_title = 'Weather Conditions',
        yaxis_title = 'Number of Casualties'
    )
    
    fig3 = px.histogram(df, x = 'pedestrian_crossing_human_control', title = 'Number of accidents per pedestrian control condition')
    fig3.update_layout(
        xaxis_title = 'Pedestrian Control',
        yaxis_title = 'Accident Count'
    )
    
    fig4 = px.histogram(df, x = 'accident_severity', title = 'Number of accidents per accident severity')
    fig4.update_layout(
        xaxis_title = 'Accident Severity',
        yaxis_title = 'Accident Count'
    )
    
    fig5 = px.histogram(df, x = 'light_conditions', title = 'Number of accidents per light condition')
    fig5.update_layout(
        xaxis_title = 'Light Condition',
        yaxis_title = 'Accident Count'
    )
    
    app = dash.Dash()

    app.layout = html.Div(children = [
        html.H1(children = 'UK Accidents 2010 Dashboard'),
        html.H2(children = 'Number of accidents in weekends vs weekdays'),
        dcc.Graph(figure = fig),
        html.H2(children = 'Number of casualties per weather condition'),
        dcc.Graph(figure = fig2),
        html.H2(children = 'Number of accidents per pedestrian control condition'),
        dcc.Graph(figure = fig3),
        html.H2(children = 'Number of accidents per accident severity'),
        dcc.Graph(figure = fig4),
        html.H2(children = 'Number of accidents per light condition'),
        dcc.Graph(figure = fig5),
    ])

    app.run_server(host='0.0.0.0', port = 8020, debug=False)
    

with DAG(
    dag_id = 'first_dag',
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
    
    load_to_postgres_task = PythonOperator(
        task_id = 'load_to_postgres_task',
        python_callable = load_to_postgres,
        op_kwargs={
            "filename": '/opt/airflow/data/finaldf.csv',
            "lookup_filename": '/opt/airflow/data/lookup_table.csv'
        }
    )
    
    # extract_figures_task = PythonOperator(
    #     task_id = "extract_figures",
    #     python_callable = extract_figures,
    #     op_kwargs={
    #         "filename": '/opt/airflow/data/finaldf.csv',
    #     }
    # )
    
    create_dashboard_task = PythonOperator(
        task_id = 'create_dashboard_task',
        python_callable = create_dashboard,
        op_kwargs={
            "filename": '/opt/airflow/data/2010_Accidents_UK.csv'
        },
    )
    
    extract_transform_load >> load_to_postgres_task >> create_dashboard_task

