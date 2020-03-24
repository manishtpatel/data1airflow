import time
import pandas as pd
import requests

from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'GST',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG('import_data', default_args=default_args)

dag.doc_md = """\
#### DAG import_data
import data sample dag, 
required conf productId, state
"""


def print_context(ds, **kwargs):
    # time.sleep(5)
    print(kwargs)
    print('conf', kwargs['conf'], kwargs["dag_run"])
    print("Remotely received value of {} for key=message".format(
        kwargs["dag_run"].conf["productId"]))
    print(ds)
    # return 'Whatever you return gets printed in the logs'


def generateProdFilters(ds, **kwargs):
    rd = pd.read_csv('http://host.docker.internal:3000/public/train.csv')
    arrayData = rd['SaleCondition'].unique()

    # data to be sent to api
    data = {'SaleCondition': arrayData,
            'productId': kwargs["dag_run"].conf["productId"]}

    # sending post request and saving response as response object
    r = requests.post(
        url='http://host.docker.internal:3000/productdata', data=data)

    # extracting response text
    pastebin_url = r.text
    print("The pastebin URL is:%s" % pastebin_url)


t1 = PythonOperator(task_id="pull_data", provide_context=True,
                    dag=dag, python_callable=print_context)
t2 = PythonOperator(task_id="setup_data", provide_context=True,
                    dag=dag, python_callable=print_context)
t3 = PythonOperator(task_id="send_okay", provide_context=True,
                    dag=dag, python_callable=print_context)
t4 = PythonOperator(task_id="index_data", provide_context=True,
                    dag=dag, python_callable=generateProdFilters)

t5 = SimpleHttpOperator(
    task_id='poststatus',
    method='POST',
    http_conn_id='http_default_test',
    endpoint='updatestatus',
    xcom_push=True,
    data={"state": 2, "completed": True,
          "productId": '{{ dag_run.conf["productId"] }}'},
    trigger_rule="all_done",
    dag=dag)

also_run_this = BashOperator(
    task_id='also_run_this',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

t1 >> t2 >> [t3, t4] >> t5


# =============================================== DAG
