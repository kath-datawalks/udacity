# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("CREATE TABLE songplays ( \
songplay_id serial PRIMARY KEY, \
start_time timestamp NOT NULL, \
user_id int NOT NULL, \
level varchar NOT NULL, \
song_id varchar, \
artist_id varchar, \
session_id int NOT NULL, \
location varchar NOT NULL, \
user_agent varchar NOT NULL \
)")

user_table_create = ("create table if not exists users (user_id int primary key, first_name varchar not null, last_name varchar, gender varchar, level varchar);")

song_table_create = ("create table if not exists songs (song_id varchar primary key, title varchar not null, artist_id varchar not null, year int not null, duration  float not null);")

artist_table_create = ("create table if not exists artists (artist_id varchar primary key, name varchar not null, location varchar, latitude float, longitude float);")

time_table_create = ("create table if not exists time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int);")

# INSERT RECORDS

songplay_table_insert = ("insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

user_table_insert = ("INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level")

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

artist_table_insert = ("INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")


time_table_insert = ("INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING")

# FIND SONGS

song_select = ("select song_id, artists.artist_id FROM artists JOIN songs ON artists.artist_id=songs.artist_id WHERE songs.title=%s AND artists.name=%s AND songs.duration = %s")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]