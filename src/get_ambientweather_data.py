import os
from pathlib import Path
from requests.exceptions import ConnectionError
import sqlite3
import sys
import time

import pandas as pd

import awdtypes
import awsecrets

# Need to run this *before* importing AmbientAPI
os.environ.update(**awsecrets.env)

from ambient_api.ambientapi import AmbientAPI

DBFILE = Path('/data/ambientweather.sqlite')

def get_devices(api):
    n = 0
    while (n := n + 1) <= 5:
        try:
            alldevices = api.get_devices()
            break
        except (IndexError, ConnectionError):
            pass
        # Sleep a little and try again
        time.sleep(5)
        # print("Trying again")
    else:
        print("System unreachable.")
        sys.exit()

    return alldevices


def add_latest_data_to_db(devicenum, device, conn):
    lastdata = device.last_data
    dtypes = awdtypes.get_awdtypes(lastdata)

    wsdata = {k: [v] for k, v in lastdata.items()}
    wsdata.pop('lastRain')  # Don't care

    for dt in list(dtypes):
        if dt not in wsdata:
            dtypes.pop(dt)

    df = pd.DataFrame.from_dict(wsdata).astype(dtypes)
    df['date'] = pd.to_datetime(df['date'])
    dftstamp = df['dateutc'].values[0]

    cur = conn.cursor()

    try:
        cur.execute(f"select dateutc from dbtable{devicenum} "
                    "order by dateutc desc limit 1")
        lasttstamp = cur.fetchone()[0]
    except sqlite3.OperationalError:
        lasttstamp = -999

    if dftstamp != lasttstamp:
        try:
            df.to_sql(f"dbtable{devicenum}", conn,
                      if_exists='append', index=False)
        except:
            # Except what?
            # The old weather station is sometimes passing
            # fields from the (disconnected) remote outdoor
            # unit that didn't exist when the table was created!
            # Update: Looks like the solar panel was still
            # providing some power. I've killed it.
            pass

    cur.close()

##############################################################################
##############################################################################
############################# START MAIN PROGRAM #############################
##############################################################################
##############################################################################

api = AmbientAPI()
alldevices = get_devices(api)

conn = sqlite3.connect(DBFILE)

for devicenum, device in enumerate(alldevices):
    add_latest_data_to_db(devicenum, device, conn)

conn.commit()
conn.close()
