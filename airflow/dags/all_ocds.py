from airflow import DAG
from datetime import timedelta
from pathlib import Path
import hashlib

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import os
import subprocess
import json
import tempfile

dir_path = Path(__file__).parent.resolve()

cli_path = dir_path / "../../ocdsdata.py"

ocdsdata_root = dir_path.parent.parent

ocdsdata_ve = ocdsdata_root / ".ve"
ocdsdata_python = ocdsdata_ve / "bin/python"

run_id = "{{run_id}}"
run_number = "{{dag_run.id}}"


def run_ocdsdata(command_str, dag_id, schema, ds=None):
    import sys

    ver = sys.version_info
    sys.path.insert(0, str(ocdsdata_ve / f"lib/python{ver.major}.{ver.minor}/site-packages"))
    sys.path.insert(0, str(ocdsdata_root))

    import datetime
    import ocdsdata

    command = getattr(ocdsdata, command_str)
    if not command:
        raise Exception(f"No command named {command_str}")

    if command_str == "scrape":
        command(dag_id, schema)
    elif command_str == "rename_schema":
        command(schema, dag_id)
    elif command_str.startswith("export"):
        command(schema, dag_id, str(datetime.datetime.utcnow())[:10])
    else:
        command(schema)

    return True


def create_dag(dag_id):

    day_of_week = int(hashlib.md5(dag_id.encode()).hexdigest(), 16) % 7

    dag = DAG(dag_id, start_date=days_ago(8), catchup=False, schedule_interval=f"0 0 * * {day_of_week}")

    schema = f"process_{dag_id}"

    with dag:
        create_schema = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="create_schema",
            op_args=["create_schema", dag_id, schema],
        )

        scrape = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="scrape",
            op_args=["scrape", dag_id, schema],
        )

        base_tables = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="create_base_tables",
            op_args=["create_base_tables", dag_id, schema],
        )

        compile_release = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="compile_releases",
            op_args=["compile_releases", dag_id, schema],
        )

        release_objects = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="release_objects",
            op_args=["release_objects", dag_id, schema],
        )

        schema_analysis = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="schema_analysis",
            op_args=["schema_analysis", dag_id, schema],
        )

        postgres_tables = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="postgres_tables",
            op_args=["postgres_tables", dag_id, schema],
        )

        export_csv = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="export_csv",
            op_args=["export_csv", dag_id, schema],
        )

        export_xlsx = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="export_xlsx",
            op_args=["export_xlsx", dag_id, schema],
        )

        export_bigquery = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="export_bigquery",
            op_args=["export_bigquery", dag_id, schema],
        )

        export_stats = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="export_stats",
            op_args=["export_stats", dag_id, schema],
        )

        rename_schema = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="rename_schema",
            op_args=["rename_schema", dag_id, schema],
        )

        export_pgdump = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="export_pgdump",
            op_args=["export_pgdump", dag_id, dag_id],
        )

        drop_schema = PythonOperator(
            python_callable=run_ocdsdata,
            task_id="drop_schema",
            op_args=["drop_schema", dag_id, dag_id],
        )

        create_schema >> scrape >> base_tables >> compile_release >> release_objects >> schema_analysis >> postgres_tables >> [
            export_csv,
            export_xlsx,
            export_bigquery,
            export_stats,
        ] >> rename_schema >> export_pgdump >> drop_schema

    return dag


scrapers = json.loads(subprocess.run([ocdsdata_python, cli_path, "export-scrapers"], capture_output=True).stdout)

for scraper in scrapers:
    globals()[scraper] = create_dag(scraper)

