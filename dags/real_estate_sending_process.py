from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

start_date = datetime.utcnow()

default_args = {
    'owner': 'lmphuc',
    'depends_on_past': False,
    'start_date': start_date,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'real_estate',
    default_args=default_args,
    description='Sending real estate information to users',
    schedule_interval=timedelta(days=1),  # Run the DAG daily
)

task1 = BashOperator(
    task_id='crawl_bds123',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/bds123.py',
    dag=dag,
)

task2 = BashOperator(
    task_id='crawl_cenhomes',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/cenhomes.py',
    dag=dag,
)

task3 = BashOperator(
    task_id='import_bds123',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/import_bds.py',
    dag=dag,
)

task4 = BashOperator(
    task_id='import_cenhomes',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/import_cenhomes.py',
    dag=dag,
)

task5 = BashOperator(
    task_id='concat_data',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/real_estate.py',
    dag=dag,
)

task6 = BashOperator(
    task_id='send_mail',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/send_mail.py',
    dag=dag,
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6
