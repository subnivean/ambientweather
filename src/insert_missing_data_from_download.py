import sys
import sqlite3
import pandas as pd

columnmap = {
 'Date': 'date',
 'Outdoor Temperature': 'tempf',
 'Feels Like': 'feelsLike',
 'Dew Point': 'dewPoint',
 'Wind Speed': 'windspeedmph',
 'Wind Gust': 'windgustmph',
 'Max Daily Gust': 'maxdailygust',
 'Wind Direction': 'winddir',
 'Hourly Rain': 'hourlyrainin',
 'Daily Rain': 'dailyrainin',
 'Weekly Rain': 'weeklyrainin',
 'Monthly Rain': 'monthlyrainin',
 'Yearly Rain': 'yearlyrainin',
 'Relative Pressure': 'baromrelin',
 'Outdoor Humidity': 'humidity',
 'Ultra-Violet Radiation Index': 'uv',
 'Indoor Temperature': 'temp1f',
 'Indoor Humidity': 'humidity1',
 'Garage Temperature': 'temp2f',
 'Garage Humidity': 'humidity2',
 'Refrigerator Temperature': 'temp4f',
 'Refrigerator Humidity': 'humidity4',
 'Absolute Pressure': 'baromabsin',
 'Outdoor Battery': 'battout',
 'Indoor Battery': 'batt1',
 'Garage Battery': 'batt2',
 'Refrigerator Battery': 'batt4',
 'Indoor Feels Like': 'feelsLike1',
 'Indoor Dew Point': 'dewPoint1',
 'Garage Feels Like': 'feelsLike2',
 'Garage Dew Point': 'dewPoint2',
 'Refrigerator Feels Like': 'feelsLike4',
 'Refrigerator Dew Point': 'dewPoint4'}

dldf = pd.read_csv('/data/ambient-weather-20220225-20220226.csv')
dldf.rename(columns=columnmap, inplace=True)
dldf['date'] = pd.to_datetime(dldf['date']).dt.tz_convert('UTC')
dldf['dateutc'] = (dldf['date'] - pd.Timestamp('1970-01-01', tz='UTC')).dt.total_seconds() * 1000

# Interpolate 1-minute values from the 5-minute values in the download
dldf = dldf.resample('1min', on='date').mean().interpolate().reset_index()
dldf['tz'] = 'America/New_York'

awconn = sqlite3.connect("/data/ambientweather.sqlite")
AWSQL = "select * from dbtable;"
awdf = pd.read_sql(AWSQL, awconn)
awdf['date'] = pd.to_datetime(awdf['date'])

# Filter the download data to only datetimes
# we don't have in the database.
mask = ~dldf['date'].isin(awdf['date'])
if mask.sum(axis=0) == 0:
    print("No records to add - quitting.")
    awconn.close()
    sys.exit(0)

dldf = dldf[mask]

# Concatenate original, missing
jdf = pd.concat([awdf, dldf])
jdf = jdf.sort_values('date')

# Clear out database table
cur = awconn.cursor()
cur.execute('DELETE FROM dbtable')
rowsbefore = cur.rowcount
awconn.commit()
cur.close()

# Write joined dataframe back to database table
rowsafter = len(jdf)
jdf.to_sql('dbtable', awconn, if_exists='append', index=False)

print(f'{rowsafter - rowsbefore} rows added.')

awconn.close()
