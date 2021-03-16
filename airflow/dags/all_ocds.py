from airflow import DAG
from datetime import timedelta
from pathlib import Path

from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago

import os
import subprocess
import json
import tempfile

dir_path = Path(__file__).parent.resolve()

cli_path = dir_path / "../../ocdsdata.py"

ocdsdata_root = (dir_path.parent.parent)
collect_requirements_path = dir_path / "../../kingfisher-collect/requirements.txt"
collect_requirements = collect_requirements_path.read_text()

requirements = [str(ocdsdata_root)]
for line in collect_requirements.split('\n'):
    if not line or line[0] in (' ', '#'):
        continue
    requirements.append(line)


run_id = "{{run_id}}"
run_number = "{{dag_run.id}}"


def run_ocdsdata(command_str, dag_id, run_number=None, run_id=None):
    import ocdsdata

    command = getattr(ocdsdata, command_str)
    if not command:
        raise Exception(f"No command named {command_str}")

    if command_str == "scrape":
        command(dag_id, f'{dag_id}_{run_number}', f'{run_id}')
    else:
        command(dag_id)

    return True


def create_dag(dag_id):

    dag = DAG(dag_id, start_date=days_ago(0))

    with dag:
        scrape = PythonVirtualenvOperator(
            python_callable=run_ocdsdata,
            task_id="scrape",
            requirements=requirements,
            op_args=["scrape", dag_id, "{{dag_run.id}}"],
        )

        base_tables = PythonVirtualenvOperator(
            python_callable=run_ocdsdata,
            task_id="create_base_tables",
            requirements=requirements,
            op_args=["create_base_tables", dag_id],
        )

        compile_release = PythonVirtualenvOperator(
            python_callable=run_ocdsdata,
            task_id="compile_releases",
            requirements=requirements,
            op_args=["compile_releases", dag_id],
        )

        release_objects = PythonVirtualenvOperator(
            python_callable=run_ocdsdata,
            task_id="release_objects",
            requirements=requirements,
            op_args=["release_objects", dag_id],
        )

        schema_analysis = PythonVirtualenvOperator(
            python_callable=run_ocdsdata,
            task_id="schema_analysis",
            requirements=requirements,
            op_args=["schema_analysis", dag_id],
        )

        scrape >> base_tables >> compile_release >> release_objects >> schema_analysis

    return dag



with tempfile.TemporaryDirectory() as tmpdirname:
    venv_dir = Path(tmpdirname) / '.ve'
    pip = venv_dir / 'bin' / 'pip'
    python = venv_dir / 'bin' / 'python'

    subprocess.run(['virtualenv', venv_dir])
    subprocess.run([pip, 'install', '-r', collect_requirements_path])
    subprocess.run([pip, 'install', ocdsdata_root])
    scrapers = json.loads(subprocess.run([python, cli_path, "export-scrapers"], capture_output=True).stdout)

for scraper in scrapers:
    globals()[scraper] = create_dag(scraper)
