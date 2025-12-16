
  
    

    create or replace table `olist-data-pipeline`.`marts`.`commercial_mart`
      
    
    

    OPTIONS()
    as (
      

-- Mart Comercial: Análises de vendas, distribuição regional, Curva ABC e faturamento
WITH revenue_by_category AS (
  SELECT
    item_category,
    SUM(item_price + order_freight_value) as total_revenue
  FROM `olist-data-pipeline`.`intermediate`.`int_order_enriched` o
  WHERE order_approved_at IS NOT NULL
  GROUP BY item_category
),

revenue_by_seller AS (
  SELECT
    seller_id,
    SUM(item_price + order_freight_value) as seller_revenue
  FROM `olist-data-pipeline`.`intermediate`.`int_order_enriched` o
  WHERE order_approved_at IS NOT NULL
  GROUP BY seller_id
),

abc_analysis_category AS (
  SELECT
    item_category,
    total_revenue,
    SUM(total_revenue) OVER () as grand_total,
    total_revenue / SUM(total_revenue) OVER () * 100 as revenue_percentage,
    SUM(total_revenue) OVER (ORDER BY total_revenue DESC) / SUM(total_revenue) OVER () * 100 as cumulative_percentage
  FROM revenue_by_category
),

abc_classification_category AS (
  SELECT
    *,
    CASE
      WHEN cumulative_percentage <= 80 THEN 'A'
      WHEN cumulative_percentage <= 95 THEN 'B'
      ELSE 'C'
    END as abc_class_category
  FROM abc_analysis_category
),

abc_analysis_seller AS (
  SELECT
    seller_id,
    seller_revenue,
    SUM(seller_revenue) OVER () as total_seller_revenue,
    seller_revenue / SUM(seller_revenue) OVER () * 100 as seller_percentage,
    SUM(seller_revenue) OVER (ORDER BY seller_revenue DESC) / SUM(seller_revenue) OVER () * 100 as seller_cumulative_percentage
  FROM revenue_by_seller
),

abc_classification_seller AS (
  SELECT
    *,
    CASE
      WHEN seller_cumulative_percentage <= 80 THEN 'A'
      WHEN seller_cumulative_percentage <= 95 THEN 'B'
      ELSE 'C'
    END as abc_class_seller
  FROM abc_analysis_seller
)

SELECT
  o.order_id,
  o.customer_id,
  o.seller_id,
  o.product_id,
  o.item_category,
  o.order_status,
  o.order_approved_at,
  o.order_delivered_customer_date,
  o.item_price,
  o.order_freight_value,

  -- Informações do seller/região
  s.seller_city,
  s.seller_state,
  s.seller_zip_code_prefix,

  -- Métricas comerciais
  o.item_price + o.order_freight_value as total_order_value,

  -- Dimensões temporais
  EXTRACT(YEAR FROM o.order_approved_at) as order_year,
  EXTRACT(MONTH FROM o.order_approved_at) as order_month,
  EXTRACT(DAY FROM o.order_approved_at) as order_day,
  FORMAT_DATE('%A', o.order_approved_at) as order_day_name,

  -- Classificação de valores
  CASE
    WHEN o.item_price < 50 THEN 'Low Value'
    WHEN o.item_price BETWEEN 50 AND 200 THEN 'Medium Value'
    WHEN o.item_price > 200 THEN 'High Value'
    ELSE 'Unknown'
  END as price_category,

  -- Análise ABC por Categoria
  abc_cat.abc_class_category,
  abc_cat.revenue_percentage as category_revenue_percentage,
  abc_cat.cumulative_percentage as category_cumulative_percentage,

  -- Análise ABC por Seller
  abc_sell.abc_class_seller,
  abc_sell.seller_percentage as seller_revenue_percentage,
  abc_sell.seller_cumulative_percentage,

  -- Análise de faturamento
  SUM(o.item_price + o.order_freight_value) OVER (
    PARTITION BY o.seller_id
    ORDER BY o.order_approved_at
    ROWS UNBOUNDED PRECEDING
  ) as seller_cumulative_revenue,

  SUM(o.item_price + o.order_freight_value) OVER (
    PARTITION BY o.item_category
    ORDER BY o.order_approved_at
    ROWS UNBOUNDED PRECEDING
  ) as category_cumulative_revenue,

  -- Região (agrupamento de estados)
  CASE
    WHEN s.seller_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
    WHEN s.seller_state IN ('RS', 'SC', 'PR') THEN 'Sul'
    WHEN s.seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN s.seller_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
    WHEN s.seller_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
    ELSE 'Outros'
  END as seller_region

FROM `olist-data-pipeline`.`intermediate`.`int_order_enriched` o
LEFT JOIN `olist-data-pipeline`.`staging`.`stg_sellers` s
  ON o.seller_id = s.seller_id
LEFT JOIN abc_classification_category abc_cat
  ON o.item_category = abc_cat.item_category
LEFT JOIN abc_classification_seller abc_sell
  ON o.seller_id = abc_sell.seller_id
WHERE o.order_approved_at IS NOT NULL
    );
  