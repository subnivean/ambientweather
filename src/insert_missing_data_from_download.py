import sys
import sqlite3
import pandas as pd

COLUMNMAP0 = {
    "Date": "date",
    "Outdoor Temperature": "tempf",
    "Feels Like": "feelsLike",
    "Dew Point": "dewPoint",
    "Wind Speed": "windspeedmph",
    "Wind Gust": "windgustmph",
    "Max Daily Gust": "maxdailygust",
    "Wind Direction": "winddir",
    "Hourly Rain": "hourlyrainin",
    "Daily Rain": "dailyrainin",
    "Weekly Rain": "weeklyrainin",
    "Monthly Rain": "monthlyrainin",
    "Yearly Rain": "yearlyrainin",
    "Relative Pressure": "baromrelin",
    "Outdoor Humidity": "humidity",
    "Ultra-Violet Radiation Index": "uv",
    "Indoor Temperature": "temp1f",
    "Indoor Humidity": "humidity1",
    "Garage Temperature": "temp2f",
    "Garage Humidity": "humidity2",
    "Refrigerator Temperature": "temp4f",
    "Refrigerator Humidity": "humidity4",
    "Absolute Pressure": "baromabsin",
    "Outdoor Battery": "battout",
    "Indoor Battery": "batt1",
    "Garage Battery": "batt2",
    "Refrigerator Battery": "batt4",
    "Indoor Feels Like": "feelsLike1",
    "Indoor Dew Point": "dewPoint1",
    "Garage Feels Like": "feelsLike2",
    "Garage Dew Point": "dewPoint2",
    "Refrigerator Feels Like": "feelsLike4",
    "Refrigerator Dew Point": "dewPoint4",
}

COLUMNMAP1 = {
    "Date": "date",
    "Simple Date": "simpledate",
    "Outdoor Temperature (°F)": "tempf",
    "Feels Like (°F)": "feelsLike",
    "Dew Point (°F)": "dewPoint",
    "Wind Speed (mph)": "windspeedmph",
    "Wind Gust (mph)": "windgustmph",
    "Max Daily Gust (mph)": "maxdailygust",
    "Wind Direction (°)": "winddir",
    "Hourly Rain (in/hr)": "hourlyrainin",
    "Event Rain (in)": "eventrainin",
    "Daily Rain (in)": "dailyrainin",
    "Weekly Rain (in)": "weeklyrainin",
    "Monthly Rain (in)": "monthlyrainin",
    "Yearly Rain (in)": "yearlyrainin",
    "Relative Pressure (inHg)": "baromrelin",
    "Humidity (%)": "humidity",
    "Ultra-Violet Radiation Index": "uv",
    "Solar Radiation (W/m^2)": "solarradiation",
    "Indoor Temperature (°F)": "tempinf",
    "Indoor Humidity (%)": "humidityin",
    "Absolute Pressure (inHg)": "baromabsin",
    "Avg Wind Direction (10 mins) (°)": "winddir_avg10m",
    "Avg Wind Speed (10 mins) (mph)": "windspdmph_avg10m",
    "Indoor Feels Like (°F)": "feelsLikein",
    "Indoor Dew Point (°F)": "dewPointin",
    "Indoor Battery": "battin",
    "Outdoor Battery": "batt",
    "CO2 battery": "batt_co2",
}


# USAGE: like this in IPython (make db copy/backup first!):
# run insert_missing_data_from_download ambient-weather-20221225-20221227.csv 1 foo.sqlite
awdumpfile, stationnum, dbfile = sys.argv[1:]

if int(stationnum) == 0:
    columnmap = COLUMNMAP0
elif int(stationnum) == 1:
    columnmap = COLUMNMAP1
else:
    print(f"Don't know station number {stationnum}!")
    1 / 0

dldf = pd.read_csv(f"/data/{awdumpfile}")
dldf.rename(columns=columnmap, inplace=True)
dldf["date"] = pd.to_datetime(dldf["date"]).dt.tz_convert("UTC")
dldf["dateutc"] = (
    dldf["date"] - pd.Timestamp("1970-01-01", tz="UTC")
).dt.total_seconds() * 1000

# Interpolate 1-minute values from the 5-minute values in the download
# so that e.g. 'average' works properly.
dldf = dldf.resample("1min", on="date").mean().interpolate().reset_index()
dldf["tz"] = "America/New_York"

awconn = sqlite3.connect(f"/data/{dbfile}")
AWSQL = f"select * from dbtable{stationnum};"
awdf = pd.read_sql(AWSQL, awconn)
awdf["date"] = pd.to_datetime(awdf["date"])

# Precision removal of 'bad' data
# awdf = awdf[~((awdf.date > "2022-12-25 14:06") & (awdf.date < "2022-12-27 14:03"))]

# Don't try to add in (new) columns from the dump
# that don't exist in the sqlite table (like when the
# battery columns were added to the new station data
# on 2022-12-25).
origcols = list(awdf.columns)
for colname in dldf.columns:
    if colname not in origcols:
        dldf.drop(colname, axis=1, inplace=True)

# Filter the download data to only datetimes
# we don't have in the database.
mask = ~dldf["date"].isin(awdf["date"])
if mask.sum(axis=0) == 0:
    print("No records to add - quitting.")
    awconn.close()
    sys.exit(0)

dldf = dldf[mask]

# Concatenate original, missing
jdf = pd.concat([awdf, dldf])
jdf = jdf.sort_values("date")

# Clear out database table
cur = awconn.cursor()
cur.execute(f"DELETE FROM dbtable{stationnum}")
rowsbefore = cur.rowcount
awconn.commit()
cur.close()

# Write joined dataframe back to database table
rowsafter = len(jdf)
jdf.to_sql(f"dbtable{stationnum}", awconn, if_exists="append", index=False)

print(f"{rowsafter - rowsbefore} rows added.")

awconn.close()
