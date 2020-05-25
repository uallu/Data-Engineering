# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "Drop TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplay(
    songplay_id   SERIAL PRIMARY KEY, 
    start_time    INT NOT NULL, 
    user_id       VARCHAR NOT NULL, 
    level         VARCHAR NOT NULL, 
    song_id       VARCHAR NOT NULL,
    artist_id     VARCHAR NOT NULL, 
    session_id    INT NOT NULL, 
    location      VARCHAR, 
    user_agent    VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE users(
    user_id    VARCHAR PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name  VARCHAR NOT NULL, 
    gender     CHAR, 
    level      VARCHAR NOT NULL
    );
""")

song_table_create = ("""
CREATE TABLE song(
    song_id   VARCHAR PRIMARY KEY, 
    title     VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year      INT, 
    duration  INT
    );
""")

artist_table_create = ("""
CREATE TABLE artist( 
    artist_id    VARCHAR PRIMARY KEY,
    name         VARCHAR NOT NULL, 
    location     VARCHAR, 
    latitude     FLOAT, 
    longitude    FLOAT 
    );
""")

time_table_create = ("""
CREATE TABLE time(
    start_time    INT PRIMARY KEY,
    hour          INT NOT NULL, 
    day           INT NOT NULL, 
    week          INT NOT NULL, 
    month         INT NOT NULL, 
    year          INT NOT NULL, 
    weekday       VARCHAR NOT NULL
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES(%s,%s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT(user_id)
    DO UPDATE
    SET level = Excluded.level;
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(song_id)
    DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artist(artist_id, name, location, latitude, longitude)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id)
    DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(start_time)
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT song.song_id, artist.artist_id FROM
    song JOIN artist ON song.artist_id = artist.artist_id
    WHERE song.title = %s 
    AND artist.name = %s
    AND song.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]