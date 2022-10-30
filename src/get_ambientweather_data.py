import os
from pathlib import Path
from requests.exceptions import ConnectionError
import sqlite3
import sys
import time

import pandas as pd

import awdtypes
import awsecrets

# DEVICENUM = 0  # Old station
DEVICENUM = 1  # New station

# Need to run this *before* importing AmbientAPI
os.environ.update(**awsecrets.env)

from ambient_api.ambientapi import AmbientAPI

DBFILE = Path('../data/ambientweather.db')

api = AmbientAPI()

n = 0
while n < 5:
    try:
        ws = api.get_devices()[DEVICENUM]
        break
    except (IndexError, ConnectionError):
        pass

    # Sleep a little and try again
    time.sleep(5)
    n += 1
    # print("Trying again")
else:
    print("System unreachable.")
    sys.exit()

lastdata = ws.last_data
dtypes = awdtypes.get_awdtypes(lastdata)

wsdata = {k: [v] for k, v in lastdata.items()}
wsdata.pop('lastRain')  # Don't care

for dt in list(dtypes):
    if dt not in wsdata:
        dtypes.pop(dt)

df = pd.DataFrame.from_dict(wsdata).astype(dtypes)
df['date'] = pd.to_datetime(df['date'])
dftstamp = df['dateutc'].values[0]

conn = sqlite3.connect(DBFILE)
cur = conn.cursor()
try:
    cur.execute('select dateutc from dbtable order by dateutc desc limit 1')
    lasttstamp = cur.fetchone()[0]
except sqlite3.OperationalError:
    lasttstamp = -999
cur.close()

if dftstamp != lasttstamp:
    df.to_sql('dbtable', conn, if_exists='append', index=False)

conn.commit()
conn.close()
