import csv
import json

import pandas as pd
import pendulum
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator


def _save_data(ti):
    api_data = ti.xcom_pull(task_ids=['get_data'])
    with open('./include/data/raw_data.json', 'w') as f:
        json.dump(api_data[0], f)


def process_data():
    with open('./include/data/raw_data.json', 'r+') as f:
        data = json.load(f)

    del data["request"]
    data['location'] = dict(city_name=data['location']['name'],
                        country_name=data['location']['country'],
                        timezone_id=data['location']['timezone_id'],
                        localtime=data['location']['localtime'])

    data['current'] = dict(observation_time=data['current']['observation_time'],
                       temperature=data['current']['temperature'],
                       weather_descriptions=data['current']['weather_descriptions'])


    with open('./include/data/edited_json.json', 'w') as f:
        json.dump(data, f)

    with open('./include/data/edited_json.json', 'r+') as f:
        json_data = json.load(f)


    location_dict = json_data['location']
    current_dict = json_data['current']
    result_dict = {**location_dict, **current_dict}


    with open('./include/data/result.csv', 'w') as f:  
        w = csv.DictWriter(f, result_dict.keys())
        w.writeheader()
        w.writerow(result_dict)


    df = pd.read_csv('./include/data/result.csv')
    df = df.rename(columns=({'localtime':'local_time'}))
    df.to_csv('./include/data/result.csv', index=False)


def load_data_to_db():
    hook = PostgresHook(postgres_conn_id='postgresql_local')
    conn = hook.get_conn()
    cur = conn.cursor()
    with open('./include/data/result.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader) # Skip the header row.
        for row in reader:
            cur.execute(
            "INSERT INTO weather_data VALUES (%s, %s, %s, %s,%s, %s, %s)",
            row
        )
    conn.commit()
    conn.close()

with DAG(
    dag_id='my_dag',
    schedule=None,
    start_date=pendulum.datetime(2022,12,2, tz="UTC"),

):
    is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='weather_api',
        endpoint='/current'
    )

    get_data = SimpleHttpOperator(
        task_id='get_data',
        http_conn_id='weather_api',
        endpoint='/current',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    save_data = PythonOperator(
        task_id='save_data',
        python_callable=_save_data
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=process_data
    )


    create_sql_table = PostgresOperator(
        task_id='create_sql_table',
        postgres_conn_id='postgresql_local',
        sql="""
        create table if not exists weather_data (
            city_name             VARCHAR(100) NOT NULL,
            country_name          VARCHAR(100) NOT NULL,
            timezone_id           VARCHAR(100)  NOT NULL,
            local_time            DATE  NOT NULL,
            observation_time      TIME  NOT NULL,
            temperature           INT   NOT NULL,
            weather_descriptions  VARCHAR(100)
            )
        """
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_db
    )


    is_api_active >> get_data >> save_data >> transform_data >>  create_sql_table >> load_data 