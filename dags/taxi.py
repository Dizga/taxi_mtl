import json
import pandas as pd
from collections import OrderedDict
from datetime import datetime
import os

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


def get_taxi_data(conn_id, endpoint, **kwargs):

    api_hook = HttpHook(http_conn_id=conn_id, method='GET')
    pg_hook = PostgresHook(http_conn_id='home_taxi_mtl')

    headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}
    response = api_hook.run(endpoint, headers=headers)
    df_taxis = pd.read_json(response.content)
    subset = df_taxis[['ads_id', 'id']][0:10]
    tuples = [tuple(x) for x in subset.values]
    pg_hook.insert_rows('public.tabletaxi', tuples, ('number', 'texttext'))
