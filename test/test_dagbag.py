from airflow.models import DagBag

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