from urllib import request

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import pendulum

with DAG(dag_id='wikipedia_view_analyze',
         start_date=pendulum.today('UTC').add(days=-1),
         schedule="@hourly",
         catchup=False,
         tags=['dag_2'],
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


    def _callable_get_data_from_wikipedia(year, month, day, hour, output_dir, **_):
        url = (f"""https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:0>2}"""
                f"""/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz""")
        print(url)
        request.urlretrieve(url, output_dir)


    refactored_get_data_from_wikipedia = PythonOperator(
        task_id="callable_get_data_from_wikipedia",
        python_callable=_callable_get_data_from_wikipedia,
        op_args=["{{execution_date.year}}", "{{execution_date.month}}",
                 "{{execution_date.day}}", "{{execution_date.hour}}", "/tmp/wikipageviwes.gz"
                 ],
            # "year": "{{execution_date.year}}",
            # "month": "{{execution_date.month}}",
            # "day": "{{execution_date.day}}",
            # "hour": "{{execution_date.hour}}",
            # "output_dir": "/tmp/wikipageviwes.gz"
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
        dag=dag
    )

    start = EmptyOperator(task_id='start')

    end = EmptyOperator(task_id='end')

    start >> refactored_get_data_from_wikipedia >> end
