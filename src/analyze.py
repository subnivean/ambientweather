import pandas as pd
import sqlite3

awconn = sqlite3.connect("../data/ambientweather.sqlite")
AWSQL = "select date, avg(tempf) from dbtable where strftime('%H', date, '-5 hours') < '06' group by date(date, '-5 hours');"
awdf = pd.read_sql(AWSQL, awconn)

print(awdf)

tsconn = sqlite3.connect('../tesla_data/energy.sqlite')
TSSQL = 'select DateTime,Home_kW,Home_kWh from energy_data;'
tsdf = pd.read_sql(TSSQL, tsconn)

print(tsdf.tail())

# print(tsdf[tsdf['Home_kW' > 7.3]])

print(tsdf.describe())
