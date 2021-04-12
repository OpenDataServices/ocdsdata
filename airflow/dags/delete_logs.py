from pathlib import Path
from airflow.utils.dates import days_ago
from airflow import DAG

from airflow.operators.bash import BashOperator

dir_path = Path(__file__).parent.resolve()

logs_dir = dir_path.parent / 'logs'

dag = DAG('delete_logs', start_date=days_ago(1), catchup=False)

with dag:
    delete_logs = BashOperator(
        bash_command=f'find {logs_dir} -mindepth 4 -size +5M -mtime +8 -delete',
        task_id="delete_logs"
    )
