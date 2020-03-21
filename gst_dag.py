from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import time

default_args = {
    'owner': 'GST',
    'depends_on_past': True,
    'start_date': days_ago(2),
}

dag = DAG('import_data', default_args=default_args)

def print_context(ds, **kwargs):
    time.sleep(5)
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

t1 = PythonOperator(task_id="pull_data", provide_context=True, dag=dag, python_callable=print_context)
t2 = PythonOperator(task_id="setup_data", provide_context=True, dag=dag, python_callable=print_context)
t3 = PythonOperator(task_id="send_okay", provide_context=True, dag=dag, python_callable=print_context)
t4 = PythonOperator(task_id="index_data", provide_context=True, dag=dag, python_callable=print_context)

t1 >> t2 >> [t3, t4]