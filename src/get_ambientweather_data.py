import os
import sqlite3
from pathlib import Path
import pandas as pd

import awdtypes
import mysecrets

# Need to run this *before* importing AmbientAPI
os.environ.update(**mysecrets.env)

from ambient_api.ambientapi import AmbientAPI

DBFILE = Path('../data/ambientweather.db')

api = AmbientAPI()
ws = api.get_devices()[0]
wsdata = {k: [v] for k, v in ws.last_data.items()}
wsdata.pop('lastRain')  # Don't care

df = pd.DataFrame.from_dict(wsdata).astype(awdtypes.DTYPES)
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