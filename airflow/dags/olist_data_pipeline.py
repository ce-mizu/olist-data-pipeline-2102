"""
DAG principal para orquestração do pipeline dbt do Olist
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.models import Variable
import os
folder = os.getcwd()

# Configurações padrão da DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Configurações do projeto dbt
DBT_PROJECT_DIR = '/opt/airflow/dbt'
DBT_PROFILES_DIR = '/opt/airflow/dbt'

# Definição da DAG
dag = DAG(
    'olist_data_pipeline',
    default_args=default_args,
    description='Pipeline de dados Olist com dbt',
    schedule=timedelta(hours=1),  # Execução a cada hora
    max_active_runs=1,
    tags=['dbt', 'olist', 'data-pipeline'],
)

# Task 0: Configurar ambiente dbt
setup_environment = BashOperator(
    task_id='setup_dbt_environment',
    bash_command=f"""
        mkdir -p /home/airflow/.dbt &&
        cd {DBT_PROJECT_DIR} &&
        ls -la profiles.yml || echo "profiles.yml not found but continuing..."
    """,
    dag=dag,
)

# Task 1: Verificar dependências do dbt
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)


# Task 2: Executar modelos de staging (incremental com batch temporal)
staging_models = BashOperator(
    task_id='run_staging_models',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --vars '{{\"batch_hours\": 1}}' --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Task 3: Executar modelos intermediários
intermediate_models = BashOperator(
    task_id='run_intermediate_models',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select intermediate --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Task 4: Executar testes de qualidade
run_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Task 5: Gerar documentação
generate_docs = BashOperator(
    task_id='generate_docs',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Definindo as dependências
setup_environment >> dbt_deps >> staging_models >> intermediate_models >> run_tests >> generate_docs
