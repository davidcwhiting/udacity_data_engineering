import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR NOT NULL,
    firstName VARCHAR,
    gender VARCHAR(1),
    itemInSession INTEGER NOT NULL,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR(4) NOT NULL,
    location SUPER,
    method VARCHAR(3) NOT NULL,
    page VARCHAR NOT NULL,
    registration BIGINT,
    sessionId INTEGER NOT NULL,
    song VARCHAR,
    status  INTEGER NOT NULL,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INTEGER,                            
    PRIMARY KEY (itemInSession, sessionId)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(20) NOT NULL,
    num_songs INTEGER NOT NULL,
    title VARCHAR NOT NULL,
    artist_name SUPER,
    artist_latitude FLOAT,
    year INTEGER NOT NULL,
    duration FLOAT,
    artist_id VARCHAR,
    artist_longitude FLOAT,
    artist_location SUPER
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER,
    level VARCHAR(4) NOT NULL,
    song_id VARCHAR(20) NOT NULL,
    artist_id VARCHAR(20),
    session_id VARCHAR,
    location SUPER,
    user_agent VARCHAR,
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY(start_time) REFERENCES time(start_time)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR(4) NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(20) NOT NULL PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR(20),
    year INTEGER NOT NULL,
    duration FLOAT,
    FOREIGN KEY(song_id) REFERENCES songs(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR,
    location SUPER,
    latitude FLOAT,
    longitude FLOAT,
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL,
    FOREIGN KEY(start_time) REFERENCES time(start_time)
)
""")

# STAGING TABLES
staging_songs_copy = ("""
copy staging_songs from {}
    credentials 'aws_iam_role={}' 
    JSON 'auto' region 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), *config['IAM_ROLE'].values())

staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}' 
    JSON {} region 'us-west-2';
""").format(config.get('S3', 'LOG_DATA'),  *config['IAM_ROLE'].values(), config.get('S3', 'LOG_JSONPATH'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + (event.ts::bigint/1000) * INTERVAL '1 second', 
    event.userId, 
    event.level, 
    song.song_id, 
    song.artist_id, 
    event.sessionid, 
    event.location, 
    event.useragent
FROM staging_events event
INNER  JOIN staging_songs song
ON event.artist = song.artist_name
""")

user_table_insert = ("""
INSERT INTO users
SELECT 
    distinct 
    userId, 
    firstName, 
    lastName,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs
SELECT 
    distinct 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT 
    distinct 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time
SELECT 
    TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second', 
    EXTRACT(hour from (TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second' )), 
    EXTRACT(day from (TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second' )), 
    extract(week from (TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second' )),
    extract(month from (TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second' )),
    extract(year from (TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second' )),
    extract(weekday from (TIMESTAMP 'epoch' + (ts::bigint/1000) * INTERVAL '1 second' ))
FROM staging_events
""")

listening_day_time = ("""
SELECT 
    CASE weekday
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 then 'Saturday'                    
    END AS weekday_name, 
    count(*) AS cnt
FROM time
GROUP BY 1
ORDER BY 2 DESC
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create ]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
