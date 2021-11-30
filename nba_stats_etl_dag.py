import datetime


from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)


default_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime.now().strftime("%Y-%m-%d"),
}
interval = '0 16 * * *'
project_id = 'projecto-331716'
dataset_name = 'dataset1'

location = 'us-central1'

sql_transform_first_table_to_staging_table_sql = (
    f"CREATE TABLE `projecto-331716.dataset1.nba_staging` AS "
    f"SELECT * "
    f"FROM `projecto-331716.dataset1.NBA_Stats` "
    f"WHERE did_not_play=0")

update_staging_table_sql = (
    f"update `projecto-331716.dataset1.nba_staging` set Inactives=null "
    f"where Inactives=''"
)

split_inactives_load_to_final_table_sql = (
    f"INSERT `projecto-331716.dataset1.NBA_Transformed_Stats_Partitioned_Clustered` "
    f"SELECT * except (Inactives),  split(Inactives, ', ') as Inactives "
    f"FROM `projecto-331716.dataset1.nba_staging` "
)


# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    "nba-stats-pipeline",
    default_args = default_args,
    schedule_interval = interval,
) as dag:
    
    listen_for_file_upload_in_cloud_storage =  GCSObjectExistenceSensor(
        bucket='bucket1nba',
        object='ASA All NBA Raw Data.csv',
        mode='poke',
        task_id="gcs_object_exists_task",
    )
    load_file_to_bigquery = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket='bucket1nba',
        source_objects=['ASA All NBA Raw Data.csv'],
        destination_project_dataset_table=f"{dataset_name}.NBA_Stats",
        schema_fields=[
            {'mode': 'NULLABLE', 'name': 'game_id', 'type': 'STRING'},
            {'mode': 'NULLABLE', 'name': 'game_date', 'type': 'DATE'}, 
            {'mode': 'NULLABLE', 'name': 'OT', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'H_A', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'Team_Abbrev', 'type': 'STRING'},
            {'mode': 'NULLABLE', 'name': 'Team_Score', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'Team_pace', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Team_efg_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Team_tov_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Team_orb_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Team_ft_rate', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Team_off_rtg', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Inactives', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_Abbrev', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_Score', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_pace', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_efg_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_tov_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_orb_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_ft_rate', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'Opponent_off_rtg', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'player', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'player_id', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'starter', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'mp', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'fg', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'fga', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'fg_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'fg3', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'fg3a', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'fg3_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'ft', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'fta', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'ft_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'orb', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'drb', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'trb', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'ast', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'stl', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'blk', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'tov', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'pf', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'pts', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'plus_minus', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'did_not_play', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'is_inactive', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'ts_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'efg_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'fg3a_per_fga_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'fta_per_fga_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'orb_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'drb_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'trb_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'ast_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'stl_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'blk_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'tov_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'usg_pct', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'off_rtg', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'def_rtg', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'bpm', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'season', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'minutes', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'double_double', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'triple_double', 'type': 'INTEGER'}, 
            {'mode': 'NULLABLE', 'name': 'DKP', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'FDP', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'SDP', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'DKP_per_minute', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'FDP_per_minute', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'SDP_per_minute', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'pf_per_minute', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'ts', 'type': 'FLOAT'}, 
            {'mode': 'NULLABLE', 'name': 'last_60_minutes_per_game_starting', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'last_60_minutes_per_game_bench', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'PG_', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'SG_', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'SF_', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'PF_', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'C_', 'type': 'STRING'}, 
            {'mode': 'NULLABLE', 'name': 'active_position_minutes', 'type': 'STRING'},
        ],
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )
    sql_transform_first_table_to_staging_table = BigQueryInsertJobOperator(
        task_id="sql_transform_first_table_to_staging_table",
        configuration={
            "query": {
                "query": sql_transform_first_table_to_staging_table_sql,
                "useLegacySql": False,
            }
        },
        location=location,
    )

    
    update_staging_table = BigQueryInsertJobOperator(
        task_id="update_staging_table",
        configuration={
            "query": {
                "query": update_staging_table_sql,
                "useLegacySql": False,
            }
        },
        location=location,
    ) 
    split_inactives_load_to_final_table = BigQueryInsertJobOperator(
        task_id="split_inactives_load_to_final_table",
        configuration={
            "query": {
                "query": split_inactives_load_to_final_table_sql,
                "useLegacySql": False,
            }
        },
        location=location,
    )
    delete_first_table = BigQueryDeleteTableOperator(
        task_id="delete-initial-loading-table",
        deletion_dataset_table=f"{project_id}.{dataset_name}.NBA_Stats",
    )
    delete_staging_table = BigQueryDeleteTableOperator(
        task_id="delete-the-staging-table",
        deletion_dataset_table=f"{project_id}.{dataset_name}.nba_staging",
    )
    delete_file = GCSDeleteObjectsOperator(
        task_id="delete_file", 
        bucket_name='bucket1nba',
        objects=['ASA All NBA Raw Data.csv']
    )


# Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    listen_for_file_upload_in_cloud_storage >> load_file_to_bigquery >> sql_transform_first_table_to_staging_table >> update_staging_table >> split_inactives_load_to_final_table >> delete_first_table >> delete_staging_table >> delete_file   
