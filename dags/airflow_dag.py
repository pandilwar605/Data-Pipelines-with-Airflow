from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

default_args = {
    'owner': 'Sanket Pandilwar',
    'start_date': datetime(2020, 7, 26),
    'depends_on_past': False,
    'retries': 3,
    'end_date': datetime(2020, 7, 30),
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift_task = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'public.staging_events',
    aws_credentials_id='aws_credentials',
    s3_bucket='s3://udacity-dend/log_data/2018/11/*.json',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift_task = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    table = 'staging_songs',
    aws_credentials_id='aws_credentials',
    s3_bucket='s3://udacity-dend/song_data/A/B/C/*.json',
    json_path='s3://udacity-dend/log_json_path.json'
)

load_songplays_table_task = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table_task = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "users",
    sql= SqlQueries.user_table_insert,
    append_only=False
)

load_song_dimension_table_task = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table_task = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    sql=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table_task = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    sql=SqlQueries.time_table_insert,
    append_only=False
)

run_quality_checks_task = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = [
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ]   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >> [stage_events_to_redshift_task, stage_songs_to_redshift_task]
[stage_events_to_redshift_task, stage_songs_to_redshift_task] >> load_songplays_table_task
load_songplays_table_task >> [load_user_dimension_table_task, load_song_dimension_table_task, load_artist_dimension_table_task,load_time_dimension_table_task] >> run_quality_checks_task
run_quality_checks_task >> end_operator