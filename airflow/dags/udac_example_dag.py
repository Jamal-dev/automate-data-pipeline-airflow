from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket_name = 'udacity-dend'
song_s3_key = "song_data"
log_s3_key = "log-data"
log_json_file = "log_json_path.json"
redshift_conn_id = "redshift"
aws_credential_id = "aws_credentials"
table_names = ["artists", "songplays", "songs", "time", "users"]

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    redshift_conn_id = redshift_conn_id,
    aws_credential_id = aws_credential_id,
    table_name = "staging_events",
    s3_bucket_name = s3_bucket_name,
    s3_file_name = log_s3_key,
    file_format = "JSON",
    log_json_file = log_json_file,
    dag = dag,
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    redshift_conn_id = redshift_conn_id,
    aws_credential_id = aws_credential_id,
    table_name = "staging_songs",
    s3_bucket_name = s3_bucket_name,
    s3_file_name = song_s3_key,
    file_format = "JSON",
    dag = dag,
    provide_context = True
)



load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = redshift_conn_id,
    sql_query = SqlQueries.songplay_table_insert, 
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn_id,
    sql_statement=SqlQueries.user_table_insert,
    table_name = "users",
    delete_load = True,
    start_date=default_args['start_date'],
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn_id,
    sql_statement=SqlQueries.song_table_insert,
    table_name = "songs",
    delete_load = True,
    start_date=default_args['start_date'],
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn_id,
    sql_statement=SqlQueries.artist_table_insert,
    table_name = "artists",
    delete_load = True,
    start_date=default_args['start_date'],
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = redshift_conn_id,
    sql_statement=SqlQueries.time_table_insert,
    table_name = "time",
    delete_load = True,
    start_date=default_args['start_date'],
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id =redshift_conn_id,
    table = table_names
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]  >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks 

run_quality_checks >> end_operator
