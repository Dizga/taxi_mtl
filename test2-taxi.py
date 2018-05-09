import json
import requests
import pandas as pd
from collections import OrderedDict
from datetime import datetime, timedelta
import psycopg2
from contextlib import closing

# %%

url_positions = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/taxi-positions/2018-03-21T20:10:00.000Z'
headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}
attributes = ['taxi', 'timestampUTC', 'status']


positions = requests.get(url_positions, headers=headers).json()
positions['items'][0]
D = OrderedDict()
for attribute in attributes:
    D[attribute] = []

for items in positions['items']:
    for item in items['items']:
        for attribute in attributes:
            D[attribute].append(item[attribute])

df_taxis_connexions = pd.DataFrame.from_dict(D)
# df_taxis_connexions.timestampUTC = pd.to_datetime(df_taxis_connexions.timestampUTC, format='%Y-%m-%dT%H:%M:%S.%fZ')
df_taxis_connexions.sort_values(["taxi", "timestampUTC"], inplace=True)
df_taxis_connexions.drop_duplicates(inplace=True)
df_taxis_connexions
# Dataframe of last connexions
df_last_connexions = df_taxis_connexions.drop_duplicates('taxi', keep='last')
df_last_connexions

# %%

conn = psycopg2.connect("host=172.18.0.2 dbname=allinall user=postgres password=postgres")
cur = conn.cursor()
sql = "INSERT INTO last_test (donnees) VALUES ('{}')".format(df_last_connexions.to_json())
try:
    cur.execute(sql)
    # tmp = cur.fetchall()
except psycopg2.Error as e:
    print(e)
conn.commit()

# %%

url_positions = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/taxi-positions/2018-03-21T20:20:00.000Z'
headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}
attributes = ['taxi', 'timestampUTC', 'status', 'operator']


positions = requests.get(url_positions, headers=headers).json()

D = OrderedDict()
for attribute in attributes:
    D[attribute] = []
D['time'] = []

for items in positions['items']:
    for item in items['items']:
        for attribute in attributes:
            D[attribute].append(item[attribute])
        D['time'].append(items['receivedAt'])

df_taxis_connexions = pd.DataFrame.from_dict(D)
df_taxis_connexions.time = pd.to_datetime(df_taxis_connexions.time, format='%Y-%m-%dT%H:%M:%S.%fZ')
df_taxis_connexions = df_taxis_connexions.append(df_last_connexions)
df_taxis_connexions.timestampUTC = pd.to_datetime(df_taxis_connexions.timestampUTC, format='%Y-%m-%dT%H:%M:%S.%fZ')
df_taxis_connexions.sort_values(["taxi", "time"], inplace=True, na_position='first')
df_taxis_connexions.drop_duplicates(['taxi', 'timestampUTC'], inplace=True)
df_taxis_connexions['status'] = [df_taxis_connexions.status.iloc[idx-1] if e else df_taxis_connexions.status.iloc[idx] for idx, e in enumerate(df_taxis_connexions.status.ne(df_taxis_connexions.status.shift()) & df_taxis_connexions.taxi.eq(df_taxis_connexions.taxi.shift()))]
df_last_connexions = df_taxis_connexions[attributes].drop_duplicates('taxi', keep='last')
df_taxis_connexions[df_taxis_connexions['taxi']=='2UWg52i']

# df_last_connexions = pd.read_pickle('dags/last_connexions1.pickle')
# df_taxis = pd.read_pickle('dags/test.pickle')
# df_taxis

# rng = pd.date_range('21-3-2018T20:00:00', periods=180, freq='S')
# df = pd.DataFrame(index=rng)
# temp = df_taxis_connexions[df_taxis_connexions.taxi == '2UWg52i'].set_index('timestampUTC')
# temp['signal'] = 1
# temp.drop(['taxi', 'status'], axis=1, inplace=True)
# temp
# df.join(temp).to_csv("donnees_test.csv")
# Dataframe of last connexions
# df_last_connexions = df_taxis_connexions.drop_duplicates('taxi', keep='last')
df_taxis_connexions["delta"] = df_taxis_connexions.groupby('taxi')['timestampUTC'].diff().fillna(5)
gb = df_taxis_connexions.groupby(['taxi', pd.Grouper(key='time', freq='T'), 'status', 'operator'])
counts = gb.size().to_frame(name='counts')
test = (counts
        .join(gb.agg({'delta': 'min'}).rename(columns={'delta': 'delta_min'}).applymap(lambda x: x.isoformat()))
        .join(gb.agg({'delta': 'max'}).rename(columns={'delta': 'delta_max'}).applymap(lambda x: x.isoformat()))
        .join(gb.agg({'delta': 'sum'}).rename(columns={'delta': 'delta_sum'}).applymap(lambda x: x.isoformat()))
        .join(gb.agg({'delta': lambda x: list(x)[0]}).rename(columns={'delta': 'last_connexion'}).fillna(5).applymap(lambda x: x.isoformat()))
        .reset_index()
        )
df_taxis_connexions[(df_taxis_connexions.timestampUTC >= '21-3-2018T20:02:00') & (df_taxis_connexions.timestampUTC <= '21-3-2018T20:05:00') & (df_taxis_connexions.taxi == 'sMDT66Z')]
test
# def test(x):
#     try:
#         return x.isoformat()
#     except
df_taxis_connexions[df_taxis_connexions['taxi']=='sMDT66Z']
df_taxis = pd.read_pickle('dags/taxis.pickle')
df_taxis[df_taxis['delta_min']=='NaT']
test.last_connexion.fillna(5)
test
[e.isoformat() for e in test.last_connexion]
str(test)
tuples = [tuple(x) for x in test.values[0:1]]
isinstance(tuples[0][6], datetime)
tuples[0][0].isoformat()
# test[(test.timestampUTC > '21-3-2018T20:00:00') & (test.timestampUTC < '21-3-2018T20:08:00')]
