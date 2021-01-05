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

# CREATE TABLES

staging_events_table_create = ("""
    --sql
    CREATE TABLE IF NOT EXISTS staging_events(
        artist VARCHAR,
        auth VARCHAR NOT NULL,
        firstName VARCHAR NOT NULL,
        gender CHAR(1),
        itemInSession INTEGER,
        lastName VARCHAR NOT NULL,
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
        userId INTEGER NOT NULL
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
        songplay_id IDENTITY(1,1) PRIMARY KEY,
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
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
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
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
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
