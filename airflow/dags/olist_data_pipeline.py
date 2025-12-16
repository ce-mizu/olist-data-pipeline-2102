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

# Task 4: Executar marts (camada final)
marts_models = BashOperator(
    task_id='run_marts_models',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Task 5: Executar testes de qualidade por camada
run_staging_tests = BashOperator(
    task_id='run_staging_tests',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select staging --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

run_intermediate_tests = BashOperator(
    task_id='run_intermediate_tests',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select intermediate --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

run_marts_tests = BashOperator(
    task_id='run_marts_tests',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select marts --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Task 6: Executar todos os testes (consolidado)
run_all_tests = BashOperator(
    task_id='run_all_tests',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --store-failures",
    dag=dag,
)

# Task 7: Gerar documentação
generate_docs = BashOperator(
    task_id='generate_docs',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt docs generate --profiles-dir {DBT_PROFILES_DIR}",
    dag=dag,
)

# Definindo as dependências
setup_environment >> dbt_deps >> staging_models >> run_staging_tests >> intermediate_models >> run_intermediate_tests >> marts_models >> run_marts_tests >> run_all_tests >> generate_docs
