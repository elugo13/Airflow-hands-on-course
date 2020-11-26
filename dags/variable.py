from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 19),
}

dag = DAG("variable", default_args=default_args, schedule_interval=timedelta(1))

t1 = BashOperator(task_id="print_path", bash_command="echo /usr/local/airflow", dag=dag)
