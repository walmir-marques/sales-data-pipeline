from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'walmir',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sales_pipeline_complete',
    default_args=default_args,
    description='API → Bronze → dbt → Gold → Power BI',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['sales', 'dataeng']
)

# 1. Ingestão API → Bronze
ingest_bronze = PythonOperator(
    task_id='ingest_api_bronze',
    python_callable=lambda: print("✅ Ingestão Python → Bronze"),
    dag=dag
)

# 2. dbt Silver
dbt_silver = BashOperator(
    task_id='dbt_silver',
    bash_command='cd D:/MEUS_PROJETOS/sales-pipeline && dbt run --select silver',
    dag=dag
)

# 3. dbt Gold
dbt_gold = BashOperator(
    task_id='dbt_gold',
    bash_command='cd D:/MEUS_PROJETOS/sales-pipeline && dbt run --select gold.*',
    dag=dag
)

# 4. Testes qualidade
dbt_tests = BashOperator(
    task_id='dbt_tests',
    bash_command='cd D:/MEUS_PROJETOS/sales-pipeline && dbt test',
    dag=dag
)

ingest_bronze >> dbt_silver >> dbt_gold >> dbt_tests