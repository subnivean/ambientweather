docker exec \
   crazy_payne \
   sqlite3 -readonly -box ../data/ambientweather.db \
     "select 
         datetime(date, '-5 hours'),tempf,temp1f,temp4f 
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

