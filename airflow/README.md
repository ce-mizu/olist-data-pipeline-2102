# Airflow para Olist Data Pipeline

Este diretório contém a configuração do Apache Airflow para orquestrar o pipeline de dados do Olist.

## 🏗️ Estrutura

```
airflow/
├── dags/                           # DAGs do Airflow
│   ├── olist_data_pipeline.py     # Pipeline básico
│   └── olist_pipeline_advanced.py # Pipeline avançado
├── logs/                          # Logs do Airflow
├── plugins/                       # Plugins customizados
├── airflow.cfg                   # Configuração do Airflow
├── docker-compose.yml            # Setup com Docker
├── Dockerfile                    # Imagem customizada
├── requirements.txt              # Dependências Python
└── init_airflow.sh              # Script de inicialização
```

## 🚀 Configuração Rápida

### Opção 1: Setup Local

```bash
# 1. Executar script de inicialização
cd airflow
./init_airflow.sh

# 2. Iniciar serviços
airflow webserver --port 8080 &
airflow scheduler &
```

### Opção 2: Setup com Docker

```bash
# 1. Construir e iniciar containers
cd airflow
docker-compose up -d

# 2. Verificar status
docker-compose ps
```

## 📊 DAGs Disponíveis

### 1. `olist_data_pipeline` (Básico)
- **Frequência**: A cada hora
- **Funcionalidades**:
  - Executa modelos staging com batch de 1 hora
  - Processa modelos intermediate
  - Executa testes de qualidade
  - Gera documentação

### 2. `olist_pipeline_advanced` (Avançado)
- **Frequência**: A cada hora
- **Funcionalidades**:
  - Batch dinâmico baseado no horário
  - Controle granular por modelo
  - Validação por etapas
  - Paralelização inteligente

## ⚙️ Configurações

### Variáveis do Airflow
```python
# Batch size dinâmico
batch_hours = Variable.get("batch_hours", default_var=1)

# Alertas por email
email_alerts = Variable.get("email_alerts", default_var=True)
```

### Conexões Necessárias
1. **BigQuery**: Configurar credentials do GCP
2. **dbt Profile**: Validar profiles.yml

## 📈 Monitoramento

### Métricas Principais
- **SLA**: Tasks devem completar em < 30 min
- **Success Rate**: > 95% das execuções
- **Data Freshness**: Dados com < 2 horas de latência

### Alertas
- Email em caso de falha
- Slack para alertas críticos
- Retry automático (2x com 5min de intervalo)

## 🛠️ Desenvolvimento

### Testando DAGs Localmente
```bash
# Validar sintaxe da DAG
python dags/olist_data_pipeline.py

# Testar task específica
airflow tasks test olist_data_pipeline staging_orders 2024-01-01
```

### Variáveis de Ambiente
```bash
export AIRFLOW_HOME=/path/to/airflow
export DBT_PROJECT_DIR=/path/to/dbt
export DBT_PROFILES_DIR=/path/to/dbt
```

## 🔒 Segurança

- Credenciais via Airflow Connections
- Secrets via Environment Variables
- RBAC habilitado para produção

## 📝 Logs

Logs estão disponíveis em:
- **Airflow UI**: http://localhost:8080
- **Sistema**: `airflow/logs/`
- **dbt**: Dentro dos logs das tasks

## 🚨 Troubleshooting

### Problemas Comuns

1. **DAG não aparece**:
   - Verificar sintaxe Python
   - Checar logs do scheduler

2. **Falha no dbt**:
   - Validar profiles.yml
   - Verificar credenciais BigQuery

3. **Timeout de tasks**:
   - Aumentar batch_hours
   - Revisar recursos disponíveis

### Comandos Úteis
```bash
# Reiniciar scheduler
airflow scheduler --daemon

# Limpar estado de DAG
airflow dags delete olist_data_pipeline

# Ver logs em tempo real
tail -f airflow/logs/scheduler/latest/scheduler.log
```

## 📞 Suporte

Para questões sobre:
- **Airflow**: Documentação oficial Apache Airflow
- **dbt**: Documentação oficial dbt
- **BigQuery**: Documentação Google Cloud Platform
