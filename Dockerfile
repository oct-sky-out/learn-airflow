FROM apache/airflow:2.7.1
ADD requirements.txt .
RUN pip install -r requirements.txt