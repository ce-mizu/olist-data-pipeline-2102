#!/bin/bash

# Script de inicialização do Airflow para o projeto Olist Data Pipeline

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Inicializando Airflow para Olist Data Pipeline${NC}"

# Diretório do projeto
PROJECT_DIR="/home/eduardomizumoto/code/ce-mizu/olist-data-pipeline-2102"
AIRFLOW_HOME="$PROJECT_DIR/airflow"

# Configurar variável de ambiente do Airflow
export AIRFLOW_HOME=$AIRFLOW_HOME

echo -e "${YELLOW}📁 Configurando diretório Airflow: $AIRFLOW_HOME${NC}"

# Criar diretórios necessários se não existirem
mkdir -p $AIRFLOW_HOME/dags
mkdir -p $AIRFLOW_HOME/logs
mkdir -p $AIRFLOW_HOME/plugins

# Instalar dependências
echo -e "${YELLOW}📦 Instalando dependências do Airflow...${NC}"
pip install -r $AIRFLOW_HOME/requirements.txt

# Inicializar banco de dados do Airflow
echo -e "${YELLOW}🗄️  Inicializando banco de dados do Airflow...${NC}"
airflow db init

# Criar usuário admin
echo -e "${YELLOW}👤 Criando usuário admin...${NC}"
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo -e "${GREEN}✅ Inicialização concluída!${NC}"
echo -e "${YELLOW}📋 Para iniciar o Airflow:${NC}"
echo -e "   1. Webserver: airflow webserver --port 8080"
echo -e "   2. Scheduler:  airflow scheduler"
echo -e "   3. Acesse:     http://localhost:8080"
echo -e "   4. Login:      admin / admin"

echo -e "${GREEN}🎯 DAGs disponíveis:${NC}"
echo -e "   • olist_data_pipeline: Pipeline básico (execução a cada hora)"
echo -e "   • olist_pipeline_advanced: Pipeline avançado com batch dinâmico"
