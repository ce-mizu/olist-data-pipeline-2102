FROM apache/airflow:3.1.5
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Dockerfile para Airflow com dbt

#FROM apache/airflow:2.8.1-python3.11

# Configurar usuário como root para instalações
#USER root

# Instalar dependências do sistema
#RUN apt-get update \
#  && apt-get install -y --no-install-recommends \
#        git \
#        wget \
#        curl \
#  && apt-get autoremove -yqq --purge \
#  && apt-get clean \
#  && rm -rf /var/lib/apt/lists/*

# Voltar para usuário airflow
#USER airflow

# Copiar requirements e instalar dependências Python
# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt

# Copiar arquivos de configuração
#COPY airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# Criar diretórios necessários
# RUN mkdir -p ${AIRFLOW_HOME}/dags \
#    && mkdir -p ${AIRFLOW_HOME}/logs \
#    && mkdir -p ${AIRFLOW_HOME}/plugins

# Configurar variáveis de ambiente
# ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
# ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
# ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
