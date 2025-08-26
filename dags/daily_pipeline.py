import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# It's good practice to ensure the 'scripts' directory is on the Python path
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_FOLDER = os.path.join(DAG_FOLDER, "scripts")
if SCRIPTS_FOLDER not in sys.path:
    sys.path.append(SCRIPTS_FOLDER)

# Now, import your main functions directly from their modules
from create_tables import main as create_tables_main
from fetch_weather import main as fetch_weather_main
from generate_sales import main as generate_sales_main


# Define the dbt function to be called by PythonOperator
def run_dbt():
    # Import the dbt CLI entry point inside the function to avoid timeout errors
    from dbt.cli.main import cli as dbt_cli_main
    
    # Set the dbt project directory
    project_dir = "/usr/local/airflow/dags/weather_sales_project"

    # Set the environment variable
    os.environ["PYTHONWARNINGS"] = "ignore"
    
    # Use dbt's internal CLI to run the models
    dbt_cli_main(args=["run", "--project-dir", project_dir, "--profiles-dir", project_dir])


with DAG(
    dag_id="daily_weather_sales_pipeline",
    start_date=datetime(2025, 8, 24),
    schedule="@daily",
    catchup=False,
    tags=["weather", "sales"],
) as dag:
    
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_main,
    )

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_main,
    )

    generate_sales_task = PythonOperator(
        task_id="generate_sales",
        python_callable=generate_sales_main,
    )
    
    run_dbt_transformation = PythonOperator(
        task_id="run_dbt_transformation",
        python_callable=run_dbt,
    )
    
    # Set the correct dependencies here
    create_tables_task >> [fetch_weather_task, generate_sales_task] >> run_dbt_transformation