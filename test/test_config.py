import glob
import importlib.util
import logging
import os
from airflow import DAG
import pytest
from airflow.utils.dag_cycle_tester import check_cycle

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", 'dags/**/*.py')
DAG_FILES = glob.glob(DAG_PATH, recursive=True)

logger = logging.getLogger("test_config")

def test_print_DAG_FILES() :
    logger.debug(len(DAG_FILES))

@pytest.mark.parametrize('dag_file', DAG_FILES)
def test_dag_integrately(dag_file):
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    dag_objs = [var for var in vars(module).values() if isinstance(var, DAG)]
    
    for dag in dag_objs:
        check_cycle(dag)
    
    assert dag_objs
    

