from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import pendulum

with DAG(
    'test',
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),
    start_date=pendulum.today('UTC').add(days=-2),
    tags=['dag_1'],
) as dag:
    start = EmptyOperator(task_id='start')
    
    python_task = PythonOperator(
        task_id="print_messages",
        python_callable=lambda: print('Hi from python operator'),
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    end = EmptyOperator(task_id='end')

    start >> python_task >> end;
