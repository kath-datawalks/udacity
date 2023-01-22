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
        artist_name varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession INTEGER,
        lastName varchar,
        length float,
        user_level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration bigint,
        session_id INTEGER,
        song_name varchar,
        status INTEGER,
        ts varchar,
        user_agent varchar,
        userid INTEGER
    )
""")

staging_songs_table_create = ("""
    create table if not exists stg_songs(
 
        song_id text,
        num_songs INT,
        title text,
        artist_name text,
        artist_latitude FLOAT,
        "year" INT,
        duration FLOAT,
        artist_id text,
        artist_longitude FLOAT,
        artist_location text
    
   /*    
    num_songs int4,
    artist_id varchar(256),
    artist_name varchar(256),
    artist_latitude numeric(18,0),
    artist_longitude numeric(18,0),
    artist_location varchar(256),
    song_id varchar(256),
    title varchar(256),
    duration numeric(18,0),
    "year" int4
*/     
    )
""")

# Fact table
songplay_table_create = ("""
    create table if not exists f_songplays(
        songplay_id INTEGER IDENTITY(0,1), 
        start_time timestamp          , 
        user_id INTEGER , 
        level varchar , 
        song_id varchar              , 
        artist_id varchar , 
        session_id INTEGER , 
        location varchar , 
        user_agent varchar 
    )
""")

# Dimension tables
user_table_create = ("""
    create table if not exists d_users(
        user_id INTEGER                sortkey ,
        first_name VARCHAR  ,
        last_name VARCHAR  ,
        gender VARCHAR  ,
        level VARCHAR  
    )
""")
song_table_create = ("""
    create table if not exists d_songs(
        song_id varchar              ,
        title varchar ,
        artist_id varchar ,
        year integer ,
        duration float 
        )
""")
artist_table_create = ("""
    create table if not exists d_artists(
        artist_id varchar PRIMARY KEY sortkey,
        name varchar, 
        location varchar, 
        latitude numeric,
        longitude numeric
        ) 
""")
time_table_create = ("""
    create table if not exists d_time(
        start_time timestamp             sortkey,
        hour INTEGER  ,
        day INTEGER  ,
        week INTEGER  ,
        month INTEGER  ,
        year INTEGER  ,
        weekday INTEGER  
    ) diststyle all
""")



# STAGING TABLES

staging_events_copy = ("""
    copy stg_events from {}
    credentials 'aws_iam_role={}'
    json {}
    TIMEFORMAT  'epochmillisecs'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy stg_songs from {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF STATUPDATE OFF
    json 'auto' 
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))


# FINAL TABLES
songplay_table_insert = ("""
    insert into f_songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select 
        TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' as start_time, 
        --e.ts as start_time,
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
        and e.song_name = s.title)
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
    insert into d_artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
        FROM stg_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    insert into d_time (start_time, hour, day, week, month, year, weekday)
    select distinct
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dayofweek from start_time)
    FROM stg_events 
    where page = 'NextSong' and ts is not null
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]