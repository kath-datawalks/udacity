import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists stg_events"
staging_songs_table_drop = "drop table if exists stg_songs"
songplay_table_drop = "drop table if exists f_songplays"
user_table_drop = "drop table if exists d_users"
song_table_drop = "drop table if exists d_songs"
artist_table_drop = "drop table if exists d_artists"
time_table_drop = "drop table if exists d_time"

# CREATE TABLES
# ODS tables

staging_events_table_create= ("""
    create table if not exists stg_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession INTEGER,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        sessionid INTEGER,
        song varchar,
        status INTEGER,
        ts varchar,
        userAgent varchar,
        userid INTEGER
    )
""")

staging_songs_table_create = ("""
    create table if not exists stg_songs(
        
        artist_id varchar,
        artist_latitude float,
        artist_location text,
        artist_longitude float,

        artist_name text,
        duration float,
        num_songs INTEGER,
        song_id varchar,
        title varchar,       
        year INTEGER
    )
""")


# Fact table

songplay_table_create = ("""
    create table if not exists f_songplays(
        songplay_id INTEGER IDENTITY(0,1), 
        start_time timestamp NOT NULL         , 
        user_id INTEGER NOT NULL, 
        level varchar NOT NULL, 
        song_id varchar NOT NULL              sortkey, 
        artist_id varchar NOT NULL, 
        session_id INTEGER NOT NULL, 
        location varchar NOT NULL, 
        user_agent varchar NOT NULL
    )
""")


# Dimension tables

user_table_create = ("""
    create table if not exists d_users(
        user_id INTEGER  NOT NULL              sortkey ,
        first_name VARCHAR  NOT NULL,
        last_name VARCHAR  NOT NULL,
        gender VARCHAR  NOT NULL,
        level VARCHAR  NOT NULL
    )
""")

song_table_create = ("""
    create table if not exists d_songs(
        song_id varchar NOT NULL             sortkey,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year integer NOT NULL,
        duration float NOT NULL
        )
""")

artist_table_create = ("""
    create table if not exists d_artists(
        artist_id VARCHAR NOT NULL,
        name VARCHAR  NOT NULL                   sortkey,
        location VARCHAR  NOT NULL,
        lattitude VARCHAR  NOT NULL,
        longitude VARCHAR  NOT NULL
        
        
        ) diststyle all
""")

time_table_create = ("""
    create table if not exists d_time(
        start_time timestamp  NOT NULL           sortkey,
        hour INTEGER  NOT NULL,
        day INTEGER  NOT NULL,
        week INTEGER  NOT NULL,
        month INTEGER  NOT NULL,
        year INTEGER  NOT NULL,
        weekday INTEGER  NOT NULL
    ) diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stg_events from {}
    credentials 'aws_iam_role={}'
    json {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy stg_events from {}
    credentials 'aws_iam_role={}'

""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    insert into f_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select 
        TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time, 
        e.userid, 
        e.user_level, 
        s.song_id, 
        s.artist_id, 
        e.session_id, 
        e.location, 
        e.user_agent
    from stg_events e
    left join stg_songs s on (
        e.artist_name = s.artist_name
        and e.spmg_title = s.title)
    where e.page = 'NextSong'
""")

user_table_insert = ("""
    insert into d_users (user_id, first_name, last_name, gender, level)
    select distinct
         userid,
         firstName,
         lastName,
         gender,
         user_level
    from stg_events
    where page = 'NextSong'     
""")

song_table_insert = ("""
    insert into d_songs (song_id, title, artist_id, year, duration)
    select distinct
        song_id, title, artist_id, year, duration
    from stg_songs
""")

artist_table_insert = ("""
    insert into d_artists (artist_id, name, location, lattitude, longitude)
    select distinct
        artist_id, artist_name, artist_location, artist_lattitude, artist_longitude
    from stg_songs

""")

time_table_insert = ("""
    insert into d_time (start_time, hour, day, week, month, year, weekday)
    select 
        ts,
        EXTRACT(hour from ts)
        EXTRACT(day from ts),
        EXTRACT(week from ts),
        EXTRACT(month from ts),
        EXTRACT(year from ts),
        EXTRACT(dow from ts)
    FROM (SELECT DISTINCT ts FROM stg_events where page = 'NextSong') t
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
