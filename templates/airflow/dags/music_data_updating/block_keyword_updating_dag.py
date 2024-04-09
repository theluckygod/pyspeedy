import datetime

import common_package.constants as constants
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="block_keyword_updating_dag",
    start_date=datetime.datetime(2023, 1, 1, tzinfo=constants.LOCAL_TIMEZONE),
    end_date=None,
    catchup=False,
    schedule_interval=None,
    tags=["updater"],
) as dag:
    stg_updater_task = BashOperator(
        task_id="stg_block_keyword_updating_task",
        bash_command="cd /home/zdeploy/KiKi/sources/python-updater/ \
            && python block_keyword_runner_airflow.py stg {{ dag_run.conf['date'] }}",
    )
    prod_updater_task = BashOperator(
        task_id="prod_block_keyword_updating_task",
        bash_command="cd /home/zdeploy/KiKi/sources/production/python-updater/ \
            && python block_keyword_runner_airflow.py prod {{ dag_run.conf['date'] }}",
    )

if __name__ == "__main__":
    dag.cli()
