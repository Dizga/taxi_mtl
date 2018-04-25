import json
import requests
import pandas as pd
from collections import OrderedDict
from datetime import datetime, timedelta
import psycopg2

url_positions = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/taxi-positions/2018-03-21T20:10:00.000Z'
url_taxis = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/taxis'
url_vehicles = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/vehicles'
url_ads = 'https://taximtl.accept.ville.montreal.qc.ca/api/data-dumps/ads'
headers = {'X-API-KEY': 'fcf1c741-e5b5-4c5e-a965-2598461b4836'}

# attributes = ['taxi', 'lat', 'lon', 'version', 'device', 'operator', 'timestampUTC', 'status', 'speed', 'azimuth']
attributes = ['taxi', 'operator', 'timestampUTC', 'status']

positions = requests.get(url_positions, headers=headers).json()

# positions = json.loads(requests.get(url_positions, headers=headers).content)

df_vehicles = pd.read_json(requests.get(url_vehicles, headers=headers).content)
df_taxis = pd.read_json(requests.get(url_taxis, headers=headers).content)
df_ads = pd.read_json(requests.get(url_ads, headers=headers).content)


# %%

conn = psycopg2.connect("host=172.20.0.4 dbname=allinall user=postgres password=postgres")
cur = conn.cursor()
try:
    cur.execute("SELECT * FROM last_connection")
    tmp = cur.fetchall()
except psycopg2.Error:
    pass
tmp
# %%

print("2018-03-21T20:01:00" > "2018-03-21T20:00:00.001Z")
date_ts = "2018-03-21T20:10:00"
d1 = datetime.strptime(date_ts, "%Y-%m-%dT%H:%M:%S")
d2 = d1 - timedelta(minutes=10)
L = []
for i in range(11):
    L.append(datetime.strftime(d2 + timedelta(minutes=i), "%Y-%m-%dT%H:%M:%S"))
L
# %%

D = OrderedDict()
for attribute in attributes:
    D[attribute] = []

for items in positions['items']:
    for item in items['items']:
        for attribute in attributes:
            D[attribute].append(item[attribute])

df_positions = pd.DataFrame.from_dict(D)
str(D['taxi'])

df_taxis = df_taxis[["ads_id", "id", "vehicle_id"]]
df_vehicles = df_vehicles[["id", "special_need_vehicle"]]
df_vehicles["special_need_vehicle"] = df_vehicles["special_need_vehicle"].map(lambda x: bool(x))
df_ads = df_ads[["id", "vdm_vignette"]]

test = pd.merge(df_positions, df_taxis, left_on=['taxi'], right_on=['id'])
test.drop(["id"], axis=1, inplace=True)

test = pd.merge(test, df_vehicles, left_on=['vehicle_id'], right_on=['id'])
test.drop(["id", "vehicle_id"], axis=1, inplace=True)

test = pd.merge(test, df_ads, left_on=['ads_id'], right_on=['id'])
test.drop(["id", "ads_id"], axis=1, inplace=True)
test
df_positions
sorted_taxi = test.sort_values(["vdm_vignette", "timestampUTC"])

L = []
previous_status = None
previous_vignette = None
previous_timestamp = None


def calcul_time_between_date(datetime1, datetime2):
    d1 = datetime.strptime(datetime1, "%Y-%m-%dT%H:%M:%S.%fZ")
    d2 = datetime.strptime(datetime2, "%Y-%m-%dT%H:%M:%S.%fZ")
    return (d2-d1).total_seconds()


for index, row in sorted_taxi.iterrows():
    if(row.vdm_vignette == previous_vignette and row.status == previous_status):
        L.append(calcul_time_between_date(previous_timestamp, row.timestampUTC))
    else:
        L.append(None)
    previous_status = row.status
    previous_vignette = row.vdm_vignette
    previous_timestamp = row.timestampUTC

sorted_taxi["delta"] = L

sorted_taxi
