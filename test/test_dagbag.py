from datetime import datetime, timedelta
from unittest import TestCase, mock
from airflow.models import DagBag
import pendulum

def test_dag_loading():
    dag_bag = DagBag(dag_folder='dags',include_examples=False)
    assert dag_bag.size() == 2
    
def test_task_dependencies():
    ## DAG, 테스크 무결성 테스트
    dag_bag = DagBag(dag_folder='dags',include_examples=False)
    dag = dag_bag.get_dag('test')
    tasks = dag.tasks
    dependencies = {
        'start': {'downstream': ['print_messages'], 'upstream': []},
        'print_messages': {'downstream': ['end'], 'upstream': ['start']},
        'end' : {'downstream': [], 'upstream': ['print_messages']},
    }
    
    for task in tasks:
        assert task.downstream_task_ids == set(dependencies[task.task_id]['downstream'])
        assert task.upstream_task_ids == set(dependencies[task.task_id]['upstream'])
        


class TestDagBackTestDag(TestCase):
    @mock.patch('airflow.operators.python.PythonOperator.execute')
    def test_task_execution(self, mock_execute):
        dag_bag = DagBag(dag_folder='dags',include_examples=False)
        
        DEFAULT_DATE = pendulum.now('UTC').add(seconds=-5)
        dag_bag.get_dag('test').get_task('start').run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        
        DEFAULT_DATE2 = pendulum.now('UTC').add(seconds=-3)
        dag_bag.get_dag('test').get_task('print_messages').run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE2)
        
        assert mock_execute.call_count == 1