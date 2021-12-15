docker run --rm \
  -v /home/pi/ambientweather/data:/data \
  -v /home/pi/ambientweather/src:/app \
   ambientweather-2

cd /home/pi/ambientweather/data && sqlite3 ambientweather.db 'select date,tempf, temp1f, temp4f from dbtable order by dateutc desc limit 1;' >> temps.log
