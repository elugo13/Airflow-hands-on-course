from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 4),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    'end_date': datetime(2020, 11, 4)
}

dag = DAG("make_dir", default_args=default_args, schedule_interval=timedelta(1))


templated_command = """
    echo "** Start Job **"
    mkdir /usr/local/airflow/dags/test_dir
    echo "** End Job **"
"""

command = "mkdir test_dir"
t1 = BashOperator(
    task_id="create_directory",
    bash_command=templated_command,
    dag=dag,
)