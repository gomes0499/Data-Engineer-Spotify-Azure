from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor

default_args = {
    'owner': 'gomes',
    'depends_on_past': False,
    "start_date": datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    "spotify-data-pipeline",
    default_args=default_args,
    description="A simple pipeline DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
)   

t1  = BashOperator(
    task_id="api",
    bash_command="source /Users/gomes/.pyenv/versions/project3/bin/activate && python /Users/gomes/Desktop/Projects/Data\ Engineer/3-Project/scripts/spotify-api.py",
    dag=dag,
)

t2  = BashOperator(
    task_id="data-ingestion",
    bash_command="source /Users/gomes/.pyenv/versions/project3/bin/activate && python /Users/gomes/Desktop/Projects/Data\ Engineer/3-Project/scripts/data-ingestion.py",
    dag=dag,
)

t3  = BashOperator(
    task_id="data-process",
    bash_command="source /Users/gomes/.pyenv/versions/project3/bin/activate && python /Users/gomes/Desktop/Projects/Data\ Engineer/3-Project/scripts/data-process.py",
    dag=dag,
)

twait = TimeDeltaSensor(
    task_id="wait_2_min_data_process",
    delta=timedelta(minutes=2),
    dag=dag,
)

t4  = BashOperator(
    task_id="move-synapses",
    bash_command="source /Users/gomes/.pyenv/versions/project3/bin/activate && python /Users/gomes/Desktop/Projects/Data\ Engineer/3-Project/scripts/synapses.py",
    dag=dag,
)

t5 = BashOperator(
    task_id='run_dbt',
    bash_command='source /Users/gomes/.pyenv/versions/project3/bin/activate && cd /Users/gomes/Desktop/Projects/Data\ Engineer/3-Project/wu3project && dbt run',
    dag=dag,
)

t1 >> t2 >> t3 >> twait >> t4 >> t5

