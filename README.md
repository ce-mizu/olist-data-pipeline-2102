# Olist Data Pipeline - DBT Project

Pipeline de dados DBT para transformar e modelar dados da plataforma de e-commerce Olist no BigQuery.

## 📁 Estrutura do Projeto

```
├── dbt/                   # Projeto DBT
│   ├── models/
│   │   ├── staging/       # Modelos de limpeza e padronização
│   │   ├── intermediate/  # Modelos de transformação intermediária
│   │   └── marts/        # Modelos finais para análise
│   ├── macros/           # Macros reutilizáveis
│   ├── tests/            # Testes customizados
│   ├── snapshots/        # Snapshots para SCD
│   ├── seeds/            # Dados estáticos
│   ├── analysis/         # Análises ad-hoc
│   ├── dbt_project.yml   # Configuração do projeto DBT
│   └── profiles.yml.template # Template de configuração de conexão
└── README.md             # Este arquivo
```

## 🚀 Setup Rápido

### 1. Configurar Ambiente
```bash
cd dbt
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configurar BigQuery
```bash
cp profiles.yml.template ~/.dbt/profiles.yml
# Edite ~/.dbt/profiles.yml com suas credenciais BigQuery
```

### 3. Executar Pipeline
```bash
# Instalar dependências
dbt deps

# Testar conexão
dbt debug

# Executar modelos
dbt run

# Executar testes
dbt test
```

## 📊 Modelos Principais

### Staging
- **stg_customers**: Clientes limpos e padronizados
- **stg_orders**: Pedidos com timestamps convertidos
- **stg_order_items**: Itens de pedidos com valores decimais
- **stg_products**: Produtos com informações padronizadas
- **stg_sellers**: Vendedores com localização
- **stg_order_payments**: Dados de pagamento
- **stg_order_reviews**: Reviews dos clientes

### Intermediate
- **int_order_enriched**: Pedidos enriquecidos com métricas calculadas

### Marts
- **dim_customers**: Dimensão de clientes com métricas agregadas
- **fct_orders**: Fatos de pedidos com categorização

## 🔧 Comandos Úteis

```bash
# Sempre ativar ambiente virtual primeiro
cd dbt && source venv/bin/activate

# Executar apenas staging
dbt run --select staging

# Executar apenas marts
dbt run --select marts

# Gerar documentação
dbt docs generate
dbt docs serve

# Executar modelo específico
dbt run --select stg_customers

# Limpar artefatos
dbt clean
```

## 📈 Principais Métricas

- **Lifetime Value**: Valor total gasto por cliente
- **Delivery Performance**: Performance de entrega (On Time, Slightly Late, Very Late)
- **Order Value Categories**: High/Medium/Low value
- **On-time Delivery Rate**: Taxa de entregas pontuais
