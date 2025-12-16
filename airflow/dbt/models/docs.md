{% docs __overview__ %}
# Olist Data Pipeline - DBT Project

Este projeto DBT transforma e modela os dados da plataforma de e-commerce Olist.

## Estrutura do Projeto

### Staging (staging/)
Contém modelos que fazem a limpeza inicial e padronização dos dados brutos:
- `stg_customers`: Dados limpos de clientes
- `stg_orders`: Dados limpos de pedidos
- `stg_order_items`: Dados limpos de itens de pedidos
- `stg_products`: Dados limpos de produtos
- `stg_sellers`: Dados limpos de vendedores

### Intermediate (intermediate/)
Contém modelos que fazem transformações intermediárias:
- `int_order_enriched`: Pedidos enriquecidos com métricas calculadas

### Marts (marts/)
Contém os modelos finais otimizados para análise:
- `dim_customers`: Dimensão de clientes com métricas agregadas
- `fct_orders`: Tabela de fatos de pedidos

## Principais Métricas

- **Lifetime Value**: Valor total gasto por cliente
- **Delivery Performance**: Performance de entrega (On Time, Slightly Late, Very Late)
- **Order Value Categories**: Categorização de valor dos pedidos

{% enddocs %}
