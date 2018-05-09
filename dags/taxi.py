import json
import pandas as pd
from collections import OrderedDict
from datetime import datetime
from contextlib import closing
import os
import errno
import logging

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


def upsert_rows(hook, table, rows, on_conflict, change_field, target_fields=None, commit_every=1000):
    """
    A generic way to insert a set of tuples into a table,
    a new transaction is created every commit_every rows

    :param table: Name of the target table
    :type table: str
    :param rows: The rows to insert into the table
    :type rows: iterable of tuples
    :param target_fields: The names of the columns to fill in the table
    :type target_fields: iterable of strings
    :param on_conflict: The names of the columns triggering the conflict
    :type on_conflict: iterable of strings
    :param change_field: The names of the columns to update on conflict
    :type target_fields: iterable of strings
    :param commit_every: The maximum number of rows to insert in one
        transaction. Set to 0 to insert all rows in one transaction.
    :type commit_every: int
    """
    if target_fields:
        target_fields = ", ".join(target_fields)
        target_fields = "({})".format(target_fields)
    else:
        target_fields = ''

    on_conflict = ", ".join(on_conflict)
    on_conflict = "({})".format(on_conflict)

    change_field = ["{0}=EXCLUDED.{0}".format(e) for e in change_field]
    change_field = ", ".join(change_field)

    with closing(hook.get_conn()) as conn:
        if hook.supports_autocommit:
            hook.set_autocommit(conn, False)

        conn.commit()

        with closing(conn.cursor()) as cur:
            for i, row in enumerate(rows, 1):
                L = []
                for cell in row:
                    L.append(hook._serialize_cell(cell, conn))
                values = tuple(L)
                placeholders = ["%s",]*len(row)
                sql = "INSERT INTO {0} {1} VALUES ({2}) ON CONFLICT {3} DO UPDATE SET {4};".format(
                    table,
                    target_fields,
                    ",".join(placeholders),
                    on_conflict,
                    change_field)
                cur.execute(sql, values)
                if commit_every and i % commit_every == 0:
                    conn.commit()

        conn.commit()


def get_taxi_data(conn_id, endpoint, **kwargs):

    api_hook = HttpHook(http_conn_id=conn_id, method='GET')

    headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}
    response = api_hook.run(endpoint, headers=headers)
    # df_taxis = pd.read_json(response.content)
    data = response.json()
    with open('dags/{}.json'.format(endpoint), 'w') as outfile:
        json.dump(data, outfile)


def get_position_taxi(conn_id, **kwargs):

    try:
        os.makedirs('taxi-positions')
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    endpoint = 'taxi-positions/{ts}.000Z'.format(**kwargs)
    # endpoint = 'taxi-positions/2018-03-21T20:20:00.000Z'
    get_taxi_data(conn_id, endpoint)


def get_last_connexions(conn_id, **kwargs):

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    sql = """SELECT taxi, "timestampUTC", status, operator FROM public.last_connexions"""
    df = pg_hook.get_pandas_df(sql)
    df.to_pickle('dags/last_connexions.pickle')


def update_last_connexions(conn_id, **kwargs):

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    attributes = ['taxi', '"timestampUTC"', 'status', 'operator']
    df_last_connexions = pd.read_pickle('dags/last_connexions.pickle')
    tuples = [tuple(x) for x in df_last_connexions.values]
    # rows = [('9876543', 'occupied', '2018-03-21T22:00:00.000Z'), ('1234567', 'occupied', '2018-03-21T22:00:00.000Z')]
    on_conflict = ['taxi', 'operator']
    change_field = ['status', '"timestampUTC"']
    upsert_rows(pg_hook, 'last_connexions', tuples, on_conflict, change_field, attributes)
    # df.to_pickle('dags/last_connexions.pickle')


def transform_taxi_data(**kwargs):
    logging.info('{ts}'.format(**kwargs))
    attributes = ['taxi', 'timestampUTC', 'status', 'operator']
    df_last_connexions = pd.read_pickle('dags/last_connexions.pickle')
    with open('dags/taxi-positions/{ts}.000Z.json'.format(**kwargs)) as data_file:
        positions = json.load(data_file)

    taxis_connexions = OrderedDict()
    for attribute in attributes:
        taxis_connexions[attribute] = []
    taxis_connexions['time'] = []

    for items in positions['items']:
        for item in items['items']:
            for attribute in attributes:
                taxis_connexions[attribute].append(item[attribute])
            taxis_connexions['time'].append(items['receivedAt'])

    df_taxis_connexions = pd.DataFrame.from_dict(taxis_connexions)
    df_taxis_connexions.time = pd.to_datetime(df_taxis_connexions.time, format='%Y-%m-%dT%H:%M:%S.%fZ')
    df_taxis_connexions.timestampUTC = pd.to_datetime(df_taxis_connexions.timestampUTC, format='%Y-%m-%dT%H:%M:%S.%fZ')
    df_taxis_connexions = df_taxis_connexions.append(df_last_connexions)
    df_taxis_connexions.sort_values(['taxi', 'time'], inplace=True, na_position='first')
    df_taxis_connexions.drop_duplicates(['taxi', 'timestampUTC'], inplace=True)
    df_taxis_connexions['status'] = [df_taxis_connexions.status.iloc[idx-1] if e else df_taxis_connexions.status.iloc[idx] for idx, e in enumerate(df_taxis_connexions.status.ne(df_taxis_connexions.status.shift()) & df_taxis_connexions.taxi.eq(df_taxis_connexions.taxi.shift()))]
    df_last_connexions = df_taxis_connexions[attributes].drop_duplicates('taxi', keep='last')
    df_taxis_connexions["delta"] = df_taxis_connexions.groupby('taxi')['timestampUTC'].diff().fillna(5)

    gb = df_taxis_connexions.groupby(['taxi', pd.Grouper(key='time', freq='T'), 'status', 'operator'])
    counts = gb.size().to_frame(name='counts')
    df_transformed_data = (counts
                           .join(gb.agg({'delta': 'min'}).rename(columns={'delta': 'delta_min'}).applymap(lambda x: x.isoformat()))
                           .join(gb.agg({'delta': 'max'}).rename(columns={'delta': 'delta_max'}).applymap(lambda x: x.isoformat()))
                           .join(gb.agg({'delta': 'sum'}).rename(columns={'delta': 'delta_sum'}).applymap(lambda x: x.isoformat()))
                           .join(gb.agg({'delta': lambda x: list(x)[0]}).rename(columns={'delta': 'last_connexion'}).applymap(lambda x: x.isoformat()))
                           .reset_index()
                           )

    df_transformed_data.to_pickle('dags/taxis.pickle')
    df_last_connexions.to_pickle('dags/last_connexions.pickle')


def load_taxi_data(conn_id, **kwargs):

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    df_taxis = pd.read_pickle('dags/taxis.pickle')
    tuples = [tuple(x) for x in df_taxis.values]
    pg_hook.insert_rows('public.table_taxi', tuples, ('taxi_id', 'time', 'status', 'operator', 'count', 'delta_min', 'delta_max', 'delta_sum', 'last_connexion'))
    # test = '{ts}.000Z'.format(**kwargs)
