# dags/daily_pipeline.py

import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# It's good practice to ensure the 'scripts' directory is on the Python path
# for direct imports if it's not automatically included.
# This assumes 'scripts' is a sibling directory to 'dags'.
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_FOLDER = os.path.join(DAG_FOLDER, "scripts")
if SCRIPTS_FOLDER not in sys.path:
    sys.path.append(SCRIPTS_FOLDER)

# Now, import your main functions directly from their modules
from fetch_weather import main as fetch_weather_main
from generate_sales import main as generate_sales_main

with DAG(
    dag_id="daily_weather_sales_pipeline",
    start_date=datetime(2025, 8, 24),
    schedule="@daily",
    catchup=False,
    tags=["weather", "sales"],
) as dag:
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_main,
    )

    generate_sales_task = PythonOperator(
        task_id="generate_sales",
        python_callable=generate_sales_main,
    )

    fetch_weather_task >> generate_sales_task