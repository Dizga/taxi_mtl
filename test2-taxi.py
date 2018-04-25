import json
import requests
import pandas as pd
from collections import OrderedDict
from datetime import datetime, timedelta
import psycopg2


def calcul_time_between_date(datetime1, datetime2):
    d1 = datetime.strptime(datetime1, "%Y-%m-%dT%H:%M:%S.%fZ")
    d2 = datetime.strptime(datetime2, "%Y-%m-%dT%H:%M:%S.%fZ")
    return (d2-d1).total_seconds()


url_positions = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/taxi-positions/2018-03-21T20:10:00.000Z'
headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}
attributes = ['taxi', 'timestampUTC', 'status']


positions = requests.get(url_positions, headers=headers).json()

D = OrderedDict()
for attribute in attributes:
    D[attribute] = []

for items in positions['items']:
    for item in items['items']:
        for attribute in attributes:
            D[attribute].append(item[attribute])

df_taxis_connexions = pd.DataFrame.from_dict(D)
df_taxis_connexions.sort_values(["taxi", "timestampUTC"], inplace=True)
df_taxis_connexions.drop_duplicates(inplace=True)

# Dataframe of last connexions
df_last_connexions = df_taxis_connexions.drop_duplicates('taxi', keep='last')


deltas = []
previous_taxi = None
previous_timestamp = None

for index, row in df_taxis_connexions.iterrows():
    if(row.taxi == previous_taxi):
        deltas.append(calcul_time_between_date(previous_timestamp, row.timestampUTC))
    else:
        deltas.append(None)
    previous_taxi = row.taxi
    previous_timestamp = row.timestampUTC

df_taxis_connexions["delta"] = deltas

date_ts = "2018-03-21T20:10:00"
d1 = datetime.strptime(date_ts, "%Y-%m-%dT%H:%M:%S")
d2 = d1 - timedelta(minutes=10)
time_range = []
for i in range(11):
    time_range.append(datetime.strftime(d2 + timedelta(minutes=i), "%Y-%m-%dT%H:%M:%S"))

for i in range(10):
    time1 = datetime.strftime(d2 + timedelta(minutes=i), "%Y-%m-%dT%H:%M:%S")
    time2 = datetime.strftime(d2 + timedelta(minutes=i+1), "%Y-%m-%dT%H:%M:%S")
