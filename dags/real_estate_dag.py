from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

start_date = datetime(2024, 1, 2)

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
    'real_estate_dags',
    default_args=default_args,
    description='Sending real estate information to users',
    schedule=timedelta(days=1),  # Run the DAG daily
)

task1 = BashOperator(
    task_id='crawl_bds123',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Extract/bds123.py',
    dag=dag,
)

task2 = BashOperator(
    task_id='crawl_cenhomes',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Extract/cenhomes.py',
    dag=dag,
)

task3 = BashOperator(
    task_id='import_bds123',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Load/import_bds.py',
    dag=dag,
)

task4 = BashOperator(
    task_id='import_cenhomes',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Load/import_cenhomes.py',
    dag=dag,
)

task5 = BashOperator(
    task_id='concat_data',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Processing/real_estate.py',
    dag=dag,
)

task6 = BashOperator(
    task_id='send_mail',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Processing/send_mail.py',
    dag=dag,
)

task7 = BashOperator(
    task_id='delete_outdated_request',
    bash_command='/home/leminhphuc/airflow_env/bin/python /home/leminhphuc/airflow_home/GR1/Processing/delete_request.py',
    dag=dag,
)

task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
