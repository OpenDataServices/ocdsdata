from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import os
import subprocess
import json

dir_path = os.path.dirname(os.path.realpath(__file__))
python_path = os.path.join(dir_path, '../../.ve/bin/python')
cli_path = os.path.join(dir_path, '../../cli.py')
run_id = '{{run_id}}'
run_number = '{{dag_run.id}}'


def create_dag(dag_id):

    dag = DAG(dag_id,
              start_date=days_ago(0)
             )

    with dag:
        t1 = BashOperator(
            task_id='scrape',
            bash_command=f'{python_path} {cli_path} run-spider {dag_id} {dag_id}_{run_number} {run_id}' 
        )

    return dag


cli_path = os.path.join(dir_path, '../../cli.py')

scrapers = json.loads(
    subprocess.run([python_path, cli_path, 'export-scrapers'], 
                   capture_output=True).stdout)

for scraper in scrapers:
    globals()[scraper] = create_dag(scraper)
