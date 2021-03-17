from airflow import DAG
from datetime import timedelta
from pathlib import Path

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
import subprocess
import json
import tempfile

dir_path = Path(__file__).parent.resolve()

cli_path = dir_path / "../../ocdsdata.py"

ocdsdata_root = dir_path.parent.parent

ocdsdata_ve = ocdsdata_root / '.ve'
ocdsdata_python = ocdsdata_ve / 'bin/python'

run_id = "{{run_id}}"
run_number = "{{dag_run.id}}"


def run_ocdsdata(command_str, dag_id, run_number=None, run_id=None):
    import sys
    ver = sys.version_info
    sys.path.insert(0, str(ocdsdata_ve / f'lib/python{ver.major}.{ver.minor}/site-packages')) 
    sys.path.insert(0, str(ocdsdata_root)) 

    import ocdsdata
    command = getattr(ocdsdata, command_str)
    if not command:
        raise Exception(f"No command named {command_str}")

    if command_str == "scrape":
        command(dag_id, f'{dag_id}_{run_number}', f'{run_id}')
    else:
        command(f'{dag_id}_{run_number}')

    return True


def create_dag(dag_id):

    dag = DAG(dag_id, start_date=days_ago(0))

    with dag:
        scrape = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="scrape",
            op_args=["scrape", dag_id, "{{dag_run.id}}"],
        )

        base_tables = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="create_base_tables",
            op_args=["create_base_tables", dag_id],
        )

        compile_release = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="compile_releases",
            op_args=["compile_releases", dag_id],
        )

        release_objects = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="release_objects",
            op_args=["release_objects", dag_id],
        )

        schema_analysis = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="schema_analysis",
            op_args=["schema_analysis", dag_id],
        )

        scrape >> base_tables >> compile_release >> release_objects >> schema_analysis

    return dag

scrapers = json.loads(subprocess.run([ocdsdata_python, cli_path, "export-scrapers"], capture_output=True).stdout)

print(scrapers)

for scraper in scrapers:
    globals()[scraper] = create_dag(scraper)
