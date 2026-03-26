from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Then gov data path
sys.path.insert(0, "/opt/airflow/gov_data")


# Clean conflicting cached modules
for mod in list(sys.modules.keys()):
    if mod in ["main", "extract", "transform", "load", "src"]:
        del sys.modules[mod]

# Force venv site-packages first
sys.path.insert(0, "/opt/airflow/site-packages")

from main import run as gov_run

def run_pipeline():
    import sys
    sys.path.insert(0, "/opt/airflow/gov_data")
    from main import run as gov_run
    gov_run()


with DAG(
    dag_id="gov_data_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gov", "etl"]
) as dag:

    run_pipeline = PythonOperator(
        task_id="run_gov_data",
        python_callable=gov_run
    )