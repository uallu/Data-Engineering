import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS public.events_stage"
staging_songs_table_drop = "DROP TABLE IF EXISTS public.songs_stage"
songplay_table_drop = "DROP TABLE IF EXISTS public.songplays"
user_table_drop = "DROP TABLE IF EXISTS public.users"
song_table_drop = "DROP TABLE IF EXISTS public.songs"
artist_table_drop = "DROP TABLE IF EXISTS public.artists"
time_table_drop = "DROP TABLE IF EXISTS public.time "

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stage_event( 
    artist_id VARCHAR, 
    auth      VARCHAR, 
    first_name VARCHAR, 
    gender     VARCHAR, 
    item_in_session VARCHAR, 
    last_name VARCHAR, 
    length FLOAT, 
    level VARCHAR, 
    location VARCHAR, 
    method VARCHAR, 
    page VARCHAR, 
    registration FLOAT, 
    session_id INTEGER, 
    song_title VARCHAR, 
    status VARCHAR, 
    ts BIGINT, 
    user_agent VARCHAR, 
    user_id FLOAT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stage_songs(
    song_id VARCHAR PRIMARY KEY, 
    title   VARCHAR, 
    duration FLOAT, 
    year     INTEGER,
    num_songs FLOAT, 
    artist_id VARCHAR, 
    artist_name VARCHAR, 
    artist_latitude VARCHAR, 
    artist_longitude VARCHAR, 
    artist_location VARCHAR
    ); 
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
 songplay_id   INTEGER IDENTITY(0,1) PRIMARY KEY,
   start_time  TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey,
      user_id  INTEGER NOT NULL REFERENCES users(user_id),
        level  VARCHAR NOT NULL,
      song_id  VARCHAR NOT NULL REFERENCES songs(song_id) distkey,
    artist_id  VARCHAR NOT NULL REFERENCES artists(artist_id),
   session_id  INTEGER NOT NULL,
     location  VARCHAR NOT NULL,
   user_agent  VARCHAR NOT NULL
   );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users( 
    user_id INTEGER NOT NULL PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name  VARCHAR NOT NULL, 
    gender     VARCHAR NOT NULL, 
    )
    DISTSTYLE auto;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR PRIMARY KEY, 
      title    VARCHAR NOT NULL, 
  artist_id     VARCHAR NOT NULL REFERENCES artists(artist_id) sortkey distkey, 
       year    INTEGER, 
  duration     FLOAT
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR PRIMARY KEY,
         name   VARCHAR(1024) NOT NULL,
     location   VARCHAR(1024),
    lattitude   FLOAT,
    longitude   FLOAT
    )
    DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time ( 
    start_time   TIMESTAMP, 
           hour  INTEGER, 
            day  INTEGER, 
           week  INTEGER, 
          month  INTEGER, 
           year  INTEGER, 
        weekday INTEGER
    )
    DISTSTYLE auto;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stage_event
FROM {}
CREDENTIALS {}
FORMAT AS JSON REGION 'us-west-2';
""").format(CONFIG["S3"]["LOG_DATA"],
            CONFIG["IAM_ROLE"]["ARN"], 
            CONFIG["S3"]["LOG_JSONPATH"]
           )


staging_songs_copy = ("""
COPY stage_songs 
FROM {} 
CREDENTIALS {}
REGION 'us-west-2'
JSON 'auto';
""").format(CONFIG["S3"]["SONG_DATA"], 
            CONFIG["IAM_ROLE"]["ARN"]
           )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time,
                    user_id,
                    level,
                    song_id, 
                    artist_id, 
                    session_id, 
                    location,
                    user_agent)
    select 
    se.ts as start_time, 
    se.user_id,
    se.level,
    ss.song_id,
    se.artist_id,
    se.session_id,
    se.location,
    se.user_agent
    FROM stage_event se
    left join stage_songs ss
    on se.song_title = ss.title
    and se.artist_id = ss.artist_id
    where se.page = 'NEXTSONG';
""")

user_table_insert = ("""
INSERT INTO public.users(user_id, first_name, last_name, gender, level)
WITH unique_user AS (
    SELECT DISTINCT se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.evel,
    ROW_NUMBER() over (partition by se.user_id order by ts desc ) as index
FROM events_stage se
)
SELECT DISTINCT user_id,
    first_name,
    last_name,
    gender,
    level
FROM unique_user
WHERE COALESCE(user_id, '') <> '' and unique_user.index = 1
and se.page = 'NEXTSONG'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
        song_id,
        title, 
        artist_id,
        year, 
        duration
  FROM stage_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
            artist_id,
            artist_name, 
            artist_location,
            artist_latitude, 
            artist_longitude
  FROM stage_songs;
""")

time_table_insert = ("""
INSERT INTO time
SELECT DISTINCT start_time,
       date_part(hour, date_time) as hour,
       date_part(day, date_time) as day,
       date_part(week, date_time) as week,
       date_part(month, date_time) as month,
       date_part(year, date_time) as year,
       date_part(weekday, date_time) as weekday
  from (select ts as start_time,
               '1970-01-01'::date + ts/1000 * interval '1 second' as date_time
          from stage_event
         group by ts) as temp
 order by start_time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
