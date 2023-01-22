Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
State and justify your database schema design and ETL pipeline.
[Optional] Provide example queries and results for song play analysis.

### Project Background

This project aims to build an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs are users are listening to.

The two source datasets are in JSON format residng in S3. The song dataset includes the master data of songs such as the artist, song name, etc., and the log dataset is about the playing records of songs such as user id, user level, song id and start time.

### Database Schema Design

A star schema is designed to optimize data analysis.
![image](https://github.com/kath-datawalks/udacity/blob/main/Data_Warehouse_with_AWS/Star%20Shema.png)

Fact Table
1. f_songplays - records in event data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
2. d_users - users in the app
- user_id, first_name, last_name, gender, level
3. d_songs - songs in music database
- song_id, title, artist_id, year, duration
4. d_artists - artists in music database
- artist_id, name, location, lattitude, longitude
5. d_time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

### Files Intro

- create_table.py is for creating the fact and dimension tables for the star schema in Redshift.
- etl.py is for loading data from S3 into staging tables on Redshift and then process that data into the analytics tables on Redshift.
- sql_queries.py is where you'll define the SQL statements, which will be imported into the two other files above.
- IaC.ipynb is for creating cluster and iam role in Redshift and clearing up resources after the whole process.
- README.md provides discussion on the ETL process and decisions for this ETL pipeline.

### Example Queries

- Top 50 songs played in 2021.

select 
    s.title as song_name,  
    count(f.songplay_id) as play_times
from f_songplay f
left join d_songs s on s.song_id = f.song_id
left join d_time t on t.start_time = f.start_time
where t.year = 2021
order by play_times desc
limit 50

- Popular artists among paid users.
select 
    a.name as artist_name,  
    count(f.songplay_id) as play_times
from f_songplay f
left join d_users u on u.user_id = f.user_id
left join d_artists a on a.artist_id = f.artist_id
where u.level = 'paid'
order by play_times desc
limit 100
