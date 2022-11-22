# Project Purpose
The project aims to build a database to store the log data and songs data in a star schema to faciliate the startup Sparkify's analysis on user behaviors. For examples, they may want to query the most popular songs and artists, the profile of their users, and the characteristics of fans for each music genre, etc..
# Star schema
There is only one fact table here which is songplays_table, and 4 dimension tables namely songs, users, artists and time referencing id of song, user, artist and start time of each song playing in the fact table.
# Files submitted
1. test.ipynb displays the first few rows of each table to check the database.
2. create_tables.py drops and creates tables. You run this file to reset your tables before each time you run your ETL scripts.
3. etl.ipynb reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. etl.py reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. sql_queries.py contains all sql queries including create, drop and insert into tables, and is imported into the last three files above.
6. README.md provides discussion on the project.
# ETL Pipeline
1. Run create_tables.py to create the database and tables.
2. Run test.ipynb to confirm the creation of the tables with the correct columns. 
3. Run etl.py to process the entire datasets.
4. Run sanity test with test.ipynb to double check if there is any mistakes.