from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.utils.dates import days_ago

with DAG(dag_id='wikipedia_view_analyze',
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=['dag_2']
) as dag:
    get_data_from_wikipedia = BashOperator(
        task_id="get_data_from_wikipedia",
        bash_command=(
            "curl -o /tmp/wikipageviwes.gz https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/{{ execution_date.year }}-"
            "{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{execution_date.year}}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
        ),
        dag=dag
    )
    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    start >> get_data_from_wikipedia >> end
