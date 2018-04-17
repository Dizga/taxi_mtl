import json
import pandas as pd
from collections import OrderedDict
from datetime import datetime
import os
import errno
import logging

from airflow.hooks.http_hook import HttpHook
from airflow.hooks.postgres_hook import PostgresHook


def calcul_time_between_date(datetime1, datetime2):
    d1 = datetime.strptime(datetime1, "%Y-%m-%dT%H:%M:%S.%fZ")
    d2 = datetime.strptime(datetime2, "%Y-%m-%dT%H:%M:%S.%fZ")
    return (d2-d1).total_seconds()


def get_taxi_data(conn_id, endpoint, **kwargs):

    api_hook = HttpHook(http_conn_id=conn_id, method='GET')

    headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}
    response = api_hook.run(endpoint, headers=headers)
    # df_taxis = pd.read_json(response.content)
    data = response.json()
    with open('{}.json'.format(endpoint), 'w') as outfile:
        json.dump(data, outfile)


def get_position_taxi(conn_id, **kwargs):

    try:
        os.makedirs('taxi-positions')
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    # endpoint = 'taxi-positions/{ts}.000Z'.format(**kwargs)
    endpoint = 'taxi-positions/2018-03-21T20:10:00.000Z'
    get_taxi_data(conn_id, endpoint)


def transform_taxi_data(**kwargs):

    attributes = ['taxi', 'operator', 'timestampUTC', 'status']

    with open('taxis.json') as data_file:
        df_taxis = pd.read_json(data_file)
    with open('ads.json') as data_file:
        df_ads = pd.read_json(data_file)
    with open('vehicles.json') as data_file:
        df_vehicles = pd.read_json(data_file)
    # with open('taxi-positions/{ts}.000Z.json'.format(**kwargs)) as data_file:
    #     positions = json.load(data_file)
    with open('taxi-positions/2018-03-21T20:10:00.000Z.json') as data_file:
        positions = json.load(data_file)

    D = OrderedDict()
    for attribute in attributes:
        D[attribute] = []

    for items in positions['items']:
        for item in items['items']:
            for attribute in attributes:
                D[attribute].append(item[attribute])

    df_positions = pd.DataFrame.from_dict(D)
    df_taxis = df_taxis[['ads_id', 'id', 'vehicle_id']]
    df_vehicles = df_vehicles[['id', 'special_need_vehicle']]
    df_vehicles['special_need_vehicle'] = df_vehicles['special_need_vehicle'].map(lambda x: bool(x))
    df_ads = df_ads[['id', 'vdm_vignette']]

    test = pd.merge(df_positions, df_taxis, left_on=['taxi'], right_on=['id'])
    test.drop(['id'], axis=1, inplace=True)

    test = pd.merge(test, df_vehicles, left_on=['vehicle_id'], right_on=['id'])
    test.drop(['id', 'vehicle_id'], axis=1, inplace=True)

    test = pd.merge(test, df_ads, left_on=['ads_id'], right_on=['id'])
    test.drop(['id', 'ads_id'], axis=1, inplace=True)
    sorted_taxi = test.sort_values(['vdm_vignette', 'timestampUTC'])

    L = []
    previous_status = None
    previous_vignette = None
    previous_timestamp = None

    for index, row in sorted_taxi.iterrows():
        if(row.vdm_vignette == previous_vignette and row.status == previous_status):
            L.append(calcul_time_between_date(previous_timestamp, row.timestampUTC))
        else:
            L.append(None)
        previous_status = row.status
        previous_vignette = row.vdm_vignette
        previous_timestamp = row.timestampUTC

    sorted_taxi["delta"] = L

    sorted_taxi.to_pickle('taxis.pickle')
    # df_taxis.to_pickle('taxis.pickle')
    # df_vehicles.to_pickle('vehicles.pickle')
    # df_ads.to_pickle('ads.pickle')


def load_taxi_data(conn_id, **kwargs):

    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    df_taxis = pd.read_pickle('taxis.pickle')
    tuples = [tuple(x) for x in df_taxis.values]
    pg_hook.insert_rows('public.taxi1', tuples, ('taxi', 'operator', 'timestamp', 'status', 'special_need', 'vignette', 'delta'))
    # test = '{ts}.000Z'.format(**kwargs)
