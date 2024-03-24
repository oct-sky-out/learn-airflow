# Airflow Test

## 1. DAG 유효성 테스트

내가 작성한 DAG가 잘 등록이되었는지 확인하려고할 때 유용.

```python
from airflow.models import DagBag

def test_dag_loading():
    dag_bag = DagBag(dag_folder='dags',include_examples=False)
    assert dag_bag.size() == 2
```

dag_folders 파라미터에 dag들이 들어있는 디렉토리로 설정한다.
include_examples=False 설정으로 기본 예제 DAG들을 무시할 수 있다.

## 2. DAG 무결성 검사

pytest를 이용해서 DAG에 이상이 없는지 확인 할 수있다.

```py
import glob
import importlib.util
import os
from airflow import DAG
import pytest
from airflow.utils.dag_cycle_tester import check_cycle

DAG_PATH = os.path.join(os.path.dirname(__file__), "..", 'dags/**/*.py') # 테스트 할 DAG, task들이 존재하는 곳으로 설정
DAG_FILES = glob.glob(DAG_PATH, recursive=True) # DAG 파일들을 DAG_PATH 기준으로 하위에 모든 파일들을 리스트로 불러온다

@pytest.mark.parametrize('dag_file', DAG_FILES)
def test_dag_integrately(dag_file):
    module_name, _ = os.path.splitext(dag_file) #
    # print(module_name);
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)
    dag_objs = [var for var in vars(module).values() if isinstance(var, DAG)]

    for dag in dag_objs:
        check_cycle(dag)

    assert dag_objs
```
