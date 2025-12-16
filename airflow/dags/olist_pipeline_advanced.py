"""
DAG avançada para orquestração do pipeline dbt do Olist com controle granular
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Configurações padrão
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
}

# Configurações do projeto
DBT_PROJECT_DIR = '/home/eduardomizumoto/code/ce-mizu/olist-data-pipeline-2102/dbt'
DBT_PROFILES_DIR = '/home/eduardomizumoto/code/ce-mizu/olist-data-pipeline-2102/dbt'

def get_batch_size(**context):
    """Função para determinar o tamanho do batch baseado no horário"""
    execution_date = context['execution_date']
    hour = execution_date.hour

    # Durante horários de pico (9-18h), usar batch menor (1 hora)
    # Durante horários normais, usar batch maior (6 horas)
    if 9 <= hour <= 18:
        return 1
    else:
        return 6

# DAG principal
dag = DAG(
    'olist_pipeline_advanced',
    default_args=default_args,
    description='Pipeline avançado Olist com controle de batch dinâmico',
    schedule=timedelta(hours=1),
    max_active_runs=1,
    tags=['dbt', 'olist', 'advanced', 'batch-control'],
)

# Início do pipeline
start_pipeline = EmptyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Instalar dependências dbt
dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag,
)

# Determinar tamanho do batch
determine_batch_size = PythonOperator(
    task_id='determine_batch_size',
    python_callable=get_batch_size,
    dag=dag,
)

# Executar staging orders com batch dinâmico
staging_orders = BashOperator(
    task_id='staging_orders',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        BATCH_HOURS={{{{ ti.xcom_pull(task_ids='determine_batch_size') }}}} &&
        dbt run --select stg_orders --profiles-dir {DBT_PROFILES_DIR} --vars "{{"batch_hours": $BATCH_HOURS"}}"
    """,
    dag=dag,
)

# Executar outros modelos de staging
staging_others = BashOperator(
    task_id='staging_others',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select staging --exclude stg_orders --profiles-dir {DBT_PROFILES_DIR}
    """,
    dag=dag,
)

# Validação de staging
validate_staging = BashOperator(
    task_id='validate_staging',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --select staging --profiles-dir {DBT_PROFILES_DIR}
    """,
    dag=dag,
)

# Executar modelos intermediate
intermediate_seller_location = BashOperator(
    task_id='intermediate_seller_location',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select int_seller_location --profiles-dir {DBT_PROFILES_DIR}
    """,
    dag=dag,
)

intermediate_order_enriched = BashOperator(
    task_id='intermediate_order_enriched',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt run --select int_order_enriched --profiles-dir {DBT_PROFILES_DIR}
    """,
    dag=dag,
)

# Validação final
final_tests = BashOperator(
    task_id='final_tests',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt test --profiles-dir {DBT_PROFILES_DIR}
    """,
    dag=dag,
)

# Gerar documentação
generate_docs = BashOperator(
    task_id='generate_docs',
    bash_command=f"""
        cd {DBT_PROJECT_DIR} &&
        dbt docs generate --profiles-dir {DBT_PROFILES_DIR}
    """,
    dag=dag,
)

# Fim do pipeline
end_pipeline = EmptyOperator(
    task_id='end_pipeline',
    dag=dag,
)

# Definindo dependências
start_pipeline >> dbt_deps >> determine_batch_size

determine_batch_size >> [staging_orders, staging_others]

[staging_orders, staging_others] >> validate_staging

validate_staging >> [intermediate_seller_location, intermediate_order_enriched]

[intermediate_seller_location, intermediate_order_enriched] >> final_tests

final_tests >> generate_docs >> end_pipeline
