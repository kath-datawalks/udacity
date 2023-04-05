from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow import conf
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries



default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023,4,4),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"

)

# create_staging_events_table = PostgresOperator(
#     task_id='Create_staging_events_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.staging_events_table_create
# )

# create_staging_songs_table = PostgresOperator(
#     task_id='Create_staging_songs_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.staging_songs_table_create
# )

# create_songplays_table = PostgresOperator(
#     task_id='Create_songplays_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.songplays_table_create
# )

# create_users_table = PostgresOperator(
#     task_id='Create_users_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.users_table_create
# )

# create_songs_table = PostgresOperator(
#     task_id='Create_songs_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.songs_table_create
# )

# create_artists_table = PostgresOperator(
#     task_id='Create_artists_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.artists_table_create
# )

# create_time_table = PostgresOperator(
#     task_id='Create_time_table',
#     dag=dag,
#     postgres_conn_id='redshift',
#     sql=SqlQueries.time_table_create
# )


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "songplays",
    sql=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,   
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,   
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    append_only=False
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# start_operator >> create_staging_events_table
# start_operator >> create_staging_songs_table
# start_operator >> create_songplays_table
# start_operator >> create_users_table
# start_operator >> create_songs_table
# start_operator >> create_artists_table
# start_operator >> create_time_table

# create_staging_events_table>> stage_events_to_redshift
# create_staging_events_table >> stage_songs_to_redshift
# create_staging_songs_table>> stage_events_to_redshift
# create_staging_songs_table >> stage_songs_to_redshift
# create_songplays_table>> stage_events_to_redshift
# create_songplays_table >> stage_songs_to_redshift
# create_users_table>> stage_events_to_redshift
# create_users_table >> stage_songs_to_redshift
# create_songs_table>> stage_events_to_redshift
# create_songs_table >> stage_songs_to_redshift
# create_artists_table>> stage_events_to_redshift
# create_artists_table >> stage_songs_to_redshift
# create_time_table>> stage_events_to_redshift
# create_time_table >> stage_songs_to_redshift

# stage_events_to_redshift>> load_songplays_table 
# stage_songs_to_redshift>> load_songplays_table

# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table

# load_user_dimension_table>> run_quality_checks
# load_song_dimension_table>> run_quality_checks
# load_artist_dimension_table>> run_quality_checks
# load_time_dimension_table>> run_quality_checks

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator