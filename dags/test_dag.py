"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from taxi import update_last_connexions
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 24),
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
    'test', default_args=default_args,
    schedule_interval='0 */10 * * 1',
    catchup=False
    )

extract_last_connexions = PythonOperator(
        task_id='extract_last_connexions',
        python_callable=update_last_connexions,
        provide_context=True,
        op_args=[
            'home_taxi_mtl'
        ],
        dag=dag
)

extract_last_connexions
