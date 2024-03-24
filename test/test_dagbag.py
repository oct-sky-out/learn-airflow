from airflow.models import DagBag

def test_dag_loading():
    dag_bag = DagBag(dag_folder='dags',include_examples=False)
    assert dag_bag.size() == 2