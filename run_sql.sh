docker run --rm \
   -v /home/pi/ambientweather/data:/data \
   ambientweather-2 \
   sqlite3 -readonly -box ../data/ambientweather.db \
     "select 
         date,tempf,temp1f,temp4f 
      from
         dbtable
      where
         dateutc > (
               select
                 dateutc
               from
                 dbtable
               order by
                 dateutc
               desc limit 1)
                 - 60 * 40 * 1000"

