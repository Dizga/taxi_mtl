"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from taxi import get_taxi_data, transform_taxi_data, load_taxi_data, get_position_taxi, get_last_connexions, update_last_connexions
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 5, 9, 15),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'taxi1', default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False
    )

extract_last_connexions = PythonOperator(
        task_id='extract_last_connexions',
        python_callable=get_last_connexions,
        provide_context=True,
        op_args=[
            'home_taxi_mtl'
        ],
        dag=dag
)

extract_data_positions = PythonOperator(
        task_id='extract_data_positions',
        python_callable=get_position_taxi,
        provide_context=True,
        op_args=[
            'taxi_mtl'
        ],
        dag=dag
)

transform_data = PythonOperator(
        task_id='transform_data_taxi',
        python_callable=transform_taxi_data,
        provide_context=True,
        dag=dag
)

update_last_connexions = PythonOperator(
        task_id='update_last_connexions',
        python_callable=update_last_connexions,
        provide_context=True,
        op_args=[
            'home_taxi_mtl'
        ],
        dag=dag
)

load_data = PythonOperator(
        task_id='load_data_taxi',
        python_callable=load_taxi_data,
        provide_context=True,
        op_args=[
            'home_taxi_mtl'
        ],
        dag=dag
)

extract_last_connexions >> transform_data
extract_data_positions >> transform_data
transform_data >> update_last_connexions
transform_data >> load_data
