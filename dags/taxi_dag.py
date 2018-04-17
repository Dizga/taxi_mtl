"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from taxi import get_taxi_data, transform_taxi_data, load_taxi_data, get_position_taxi
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 17),
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
    'taxi', default_args=default_args,
    schedule_interval='0 */10 * * *'
    # "*/10 * * * *"
    )

# t1, t2 and t3 are examples of tasks created by instantiating operators
extract_data_taxis = PythonOperator(
        task_id='extract_data_taxis',
        python_callable=get_taxi_data,
        provide_context=True,
        op_args=[
            'taxi_mtl',
            'taxis'
        ],
        dag=dag
)
extract_data_ads = PythonOperator(
        task_id='extract_data_ads',
        python_callable=get_taxi_data,
        provide_context=True,
        op_args=[
            'taxi_mtl',
            'ads'
        ],
        dag=dag
)
extract_data_vehicles = PythonOperator(
        task_id='extract_data_vehicles',
        python_callable=get_taxi_data,
        provide_context=True,
        op_args=[
            'taxi_mtl',
            'vehicles'
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

load_data = PythonOperator(
        task_id='load_data_taxi',
        python_callable=load_taxi_data,
        provide_context=True,
        op_args=[
            'home_taxi_mtl'
        ],
        dag=dag
)

extract_data_ads >> transform_data >> load_data
extract_data_taxis >> transform_data
extract_data_vehicles >> transform_data
extract_data_positions >> transform_data
