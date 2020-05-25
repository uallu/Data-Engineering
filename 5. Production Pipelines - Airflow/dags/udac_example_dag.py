from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, 
                              PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False, 
    'retries': 3, 
    'retry_delay': timedelta(mins=5),
    'catchup': True, 
    'email_on_rerty': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

#dummy operator "Begin_execution"
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#task for creating tables, using PostgresOperator for Redshift
create_tables = PostgresOperator(
                                 task_id = "create_tables", 
                                 dag = dag, 
                                 postgres_conn_id="redshift",
                                 sql = "create_tables.sql")
                                

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag, 
    redshift_conn_id = 'redshift', 
    aws_credentials_id = 'aws_credentials', 
    table = 'staging_events', 
    s3_bucket='udacity-dend', 
    s3_key='log_data'  
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag, 
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift', 
    s3_bucket = 'udacity-dend', 
    s3_key = 'song_data'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag, 
    redshift_conn_id = 'redshift',
    table="songplay",
    sql = SqlQueries.songplay_table_insert, 
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    redshift_conn_id = 'redshift',
    table="users",
    sql = SqlQueries.user_table_insert, 
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag, 
    table="song",
    redshift_conn_id = 'redshift',
    sql = SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag, 
    table="artist",
    redshift_conn_id = 'redshift',
    sql = SqlQueries.artist_table_insert, 
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag, 
    table="time",
    redshift_conn_id = 'redshift',
    sql = SqlQueries.time_table_insert,
    append_only=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag, 
    provide_context = True, 
    tables=['artists', 'songplays', 'songs', 'users']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#task for creating tables 
start_operator >> create_tables

#tasks for loading data to redshift
create_tables >> stage_events_to_redshift >> load_songplays_table
create_tables >> stage_songs_to_redshift >> load_songplays_table

#tasks for loading user,song,artist,time table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

#tasks for running quality checks
run_quality_checks >> end_operator



