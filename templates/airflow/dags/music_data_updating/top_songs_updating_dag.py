import datetime

import common_package.constants as constants
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="top_songs_updating_dag",
    start_date=datetime.datetime(2022, 8, 1, tzinfo=constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval="00 03 * * *",
    tags=["updater"],
) as dag:
    updater_task = BashOperator(
        task_id="top_songs_updating_task",
        bash_command=f"cd /home/zdeploy/KiKi/sources/music-personalization/ \
            && bash top_songs_updater.sh {constants.DEPLOY_TYPE}",
    )

if __name__ == "__main__":
    dag.cli()
