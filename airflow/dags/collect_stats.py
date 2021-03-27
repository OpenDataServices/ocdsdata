from pathlib import Path
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.python import PythonOperator

dir_path = Path(__file__).parent.resolve()

ocdsdata_root = dir_path.parent.parent
ocdsdata_ve = ocdsdata_root / ".ve"


def run_collect_stats():
    import sys

    ver = sys.version_info
    sys.path.insert(0, str(ocdsdata_ve / f"lib/python{ver.major}.{ver.minor}/site-packages"))
    sys.path.insert(0, str(ocdsdata_root))

    import ocdsdata
    ocdsdata.collect_stats()


dag = DAG('collect_stats', start_date=days_ago(1), catchup=False)

with dag:
    collect_stats = PythonOperator(
        python_callable=run_collect_stats,
        task_id="collect_stats"
    )
