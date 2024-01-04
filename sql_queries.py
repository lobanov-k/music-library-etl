import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_DATA = config.get('S3','LOG_DATA')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS log_data;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS song_data;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE log_data (
        artist VARCHAR,
        auth VARCHAR(20),
        first_name VARCHAR,
        gender VARCHAR(1), 
        item_in_session INTEGER,
        last_name VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR(100),
        method VARCHAR(10),
        page VARCHAR,
        registration VARCHAR,
        session_id INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE song_data (
        num_songs INTEGER,
        artist_id VARCHAR(2056),
        artist_latitude REAL,
        artist_longitude REAL, 
        artist_location VARCHAR(2056),
        artist_name VARCHAR(2056),
        song_id VARCHAR(2056),
        title VARCHAR(2056),
        duration REAL,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR sortkey distkey,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR(100),
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER PRIMARY KEY sortkey,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE song (
        song_id VARCHAR PRIMARY KEY distkey sortkey,
        title VARCHAR(2056),
        artist_id VARCHAR,
        year INTEGER,
        duration REAL
    );
""")

artist_table_create = ("""
    CREATE TABLE artist (
        artist_id VARCHAR sortkey,
        name VARCHAR(2056),
        location VARCHAR(2056),
        latitude REAL,
        longitude REAL
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY sortkey,
        hour SMALLINT NOT NULL,
        day SMALLINT NOT NULL,
        week SMALLINT NOT NULL,
        month SMALLINT NOT NULL,
        year SMALLINT NOT NULL,
        weekday SMALLINT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
  copy log_data from '{}'
  credentials 'aws_iam_role={}'
  region 'us-west-2'
  json '{}'
  compupdate off
  dateformat as 'auto'
  timeformat as 'auto'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
  copy song_data from '{}'
  credentials 'aws_iam_role={}'
  region 'us-west-2'
  json 'auto'
  compupdate off
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplay (
    start_time, user_id, level,
    song_id, artist_id, session_id,
    location, user_agent
  )
  SELECT
    TIMESTAMP 'epoch' + (l.ts/1000) * interval '1 second',
    l.user_id, l.level, s.song_id, s.artist_id,
    l.session_id, l.location, l.user_agent
  FROM log_data l
  LEFT JOIN song_data s
  ON l.song = s.title AND l.artist = s.artist_name
  WHERE l.song != 'None'
""")

user_table_insert = ("""
  INSERT INTO users (
    user_id, first_name, last_name, gender, level
  )
  SELECT DISTINCT (user_id)
    user_id, first_name, last_name, gender, level
  FROM log_data
  WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
  INSERT INTO song (
    song_id, title, artist_id, year, duration
  )
  SELECT
    song_id, title, artist_id, year, duration
  FROM song_data
""")

artist_table_insert = ("""
INSERT INTO artist (
  artist_id, name, location, latitude, longitude
)
SELECT DISTINCT (artist_id)
  artist_id, artist_name as name, artist_location as location,
  artist_latitude as latitude, artist_longitude as longitude
FROM song_data
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
  INSERT INTO time (
    start_time, hour, day, week, month, year, weekday
  ) 
  SELECT ts.start_time,
       EXTRACT(HOUR FROM ts.start_time),
       EXTRACT(DAY FROM ts.start_time),
       EXTRACT(WEEK FROM ts.start_time),
       EXTRACT(MONTH FROM ts.start_time),
       EXTRACT(YEAR FROM ts.start_time),
       EXTRACT(WEEKDAY FROM ts.start_time) AS weekday
  FROM (
    SELECT
      TIMESTAMP 'epoch' + (ts/1000) * interval '1 second' as start_time
    FROM log_data
  ) ts
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_final_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries_map = [['log_data', staging_events_copy], ['song_data', staging_songs_copy]]
insert_table_queries_map = [
  ['songplay', songplay_table_insert],
  ['users', user_table_insert],
  ['song', song_table_insert],
  ['artist', artist_table_insert],
  ['time', time_table_insert]
]
# insert_table_queries = [time_table_insert]
