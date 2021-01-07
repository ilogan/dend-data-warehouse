import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
config_s3 = config['S3']
config_iam = config['IAM_ROLE']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR NOT NULL,
        firstName VARCHAR,
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR NOT NULL,
        location VARCHAR,
        method VARCHAR NOT NULL,
        page VARCHAR NOT NULL,
        registration TIMESTAMP,
        sessionId INTEGER NOT NULL,
        song VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS staging_songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        year INTEGER,
        duration FLOAT,
        artist_id VARCHAR NOT NULL,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_name VARCHAR NOT NULL,
        num_songs INTEGER
    );
""")

songplay_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR,
        FOREIGN KEY(start_time) REFERENCES time(start_time),
        FOREIGN KEY(user_id) REFERENCES users(user_id),
        FOREIGN KEY(song_id) REFERENCES songs(song_id),
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    );
""")

user_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS users(
        user_id INTEGER PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR NOT NULL
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER,
        duration FLOAT,
        FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
    ) DISTSTYLE ALL;
""")

artist_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    ) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    --sql
    COPY staging_events
    FROM {}
    CREDENTIALS {}
    REGION 'us-west-2'
    TIMEFORMAT 'epochmillisecs'
    FORMAT AS JSON {};
""").format(config_s3['LOG_DATA'],
            config_iam['ARN'],
            config_s3['LOG_JSONPATH'])

staging_songs_copy = ("""
    --sql
    COPY staging_songs
    FROM {}
    CREDENTIALS {}
    REGION 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(config_s3['SONG_DATA'], config_iam['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    --sql
    INSERT INTO songplays(
        start_time,
        user_id,
        level,
        session_id,
        user_agent,
        location,
        song_id,
        artist_id
    )
    SELECT DISTINCT e.ts,
                    e.userId,
                    e.level,
                    e.sessionId,
                    e.userAgent,
                    e.location,
                    s.song_id,
                    s.artist_id
    FROM staging_events AS e
        JOIN staging_songs AS s
        ON e.song = s.title
            AND e.artist = s.artist_name
    WHERE page = 'NextSong';
""")

user_table_insert = ("""
    --sql
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong';
""")

song_table_insert = ("""
    --sql
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    --sql
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    --sql
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        ts,
        EXTRACT(hour FROM ts),     -- integer 0-23
        EXTRACT(day FROM ts),      -- integer 1-31
        EXTRACT(week FROM ts),     -- integer 1-53 (iso)
        EXTRACT(month FROM ts),    -- integer 1-12
        EXTRACT(year FROM ts),  -- integer yyyy (iso)
        EXTRACT(dow FROM ts)   -- integer 1-7 (iso)
    FROM staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    time_table_create,
    song_table_create,
    songplay_table_create]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop, song_table_drop,
    artist_table_drop,
    time_table_drop]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert]
