
  
    

    create or replace table `olist-data-pipeline`.`marts`.`logistical_mart`
      
    
    

    OPTIONS()
    as (
      

-- Mart Logístico: Análises de entrega, prazos, frete e distribuição regional
WITH delivery_stats_by_region_category AS (
  SELECT
    seller_region,
    customer_region,
    item_category,
    AVG(CASE WHEN days_to_carrier > 0 THEN days_to_carrier END) as avg_days_to_carrier,
    AVG(CASE WHEN days_in_transit > 0 THEN days_in_transit END) as avg_days_in_transit,
    AVG(CASE WHEN total_delivery_days > 0 THEN total_delivery_days END) as avg_total_delivery_days,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN total_delivery_days <= 7 THEN 1 END) as fast_deliveries,
    COUNT(CASE WHEN total_delivery_days > 15 THEN 1 END) as slow_deliveries
  FROM (
    SELECT
      o.order_id,
      o.item_category,
      DATE_DIFF(o.order_delivered_carrier_date, o.order_approved_at, DAY) as days_to_carrier,
      DATE_DIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date, DAY) as days_in_transit,
      DATE_DIFF(o.order_delivered_customer_date, o.order_approved_at, DAY) as total_delivery_days,
      CASE
        WHEN s.seller_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
        WHEN s.seller_state IN ('RS', 'SC', 'PR') THEN 'Sul'
        WHEN s.seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
        WHEN s.seller_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
        WHEN s.seller_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
        ELSE 'Outros'
      END as seller_region,
      CASE
        WHEN c.customer_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
        WHEN c.customer_state IN ('RS', 'SC', 'PR') THEN 'Sul'
        WHEN c.customer_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
        WHEN c.customer_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
        WHEN c.customer_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
        ELSE 'Outros'
      END as customer_region
    FROM `olist-data-pipeline`.`intermediate`.`int_order_enriched` o
    LEFT JOIN `olist-data-pipeline`.`staging`.`stg_sellers` s ON o.seller_id = s.seller_id
    LEFT JOIN `olist-data-pipeline`.`staging`.`stg_customers` c ON o.customer_id = c.customer_id
    WHERE o.order_approved_at IS NOT NULL
  ) sub
  GROUP BY seller_region, customer_region, item_category
)

SELECT
  o.order_id,
  o.customer_id,
  o.seller_id,
  o.item_category,
  o.order_status,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_freight_value,

  -- Dados de região (seller e customer)
  s.seller_city,
  s.seller_state,
  c.customer_city,
  c.customer_state,

  -- Regiões agregadas
  CASE
    WHEN s.seller_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
    WHEN s.seller_state IN ('RS', 'SC', 'PR') THEN 'Sul'
    WHEN s.seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN s.seller_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
    WHEN s.seller_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
    ELSE 'Outros'
  END as seller_region,

  CASE
    WHEN c.customer_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
    WHEN c.customer_state IN ('RS', 'SC', 'PR') THEN 'Sul'
    WHEN c.customer_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN c.customer_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
    WHEN c.customer_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
    ELSE 'Outros'
  END as customer_region,

  -- Identificação de mesmo estado/região
  CASE
    WHEN s.seller_state = c.customer_state THEN 'Same State'
    WHEN s.seller_state != c.customer_state THEN 'Different State'
    ELSE 'Unknown'
  END as state_delivery_type,

  -- Cálculos logísticos
  DATE_DIFF(o.order_delivered_carrier_date, o.order_approved_at, DAY) as days_to_carrier,
  DATE_DIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date, DAY) as days_in_transit,
  DATE_DIFF(o.order_delivered_customer_date, o.order_approved_at, DAY) as total_delivery_days,

  -- Análise de pernas (Fornecedor/Transportadora)
  CASE
    WHEN DATE_DIFF(o.order_delivered_carrier_date, o.order_approved_at, DAY) <= 2 THEN 'Fast Pickup'
    WHEN DATE_DIFF(o.order_delivered_carrier_date, o.order_approved_at, DAY) BETWEEN 3 AND 5 THEN 'Standard Pickup'
    WHEN DATE_DIFF(o.order_delivered_carrier_date, o.order_approved_at, DAY) > 5 THEN 'Slow Pickup'
    ELSE 'Unknown Pickup'
  END as supplier_leg_performance,

  CASE
    WHEN DATE_DIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date, DAY) <= 3 THEN 'Fast Transit'
    WHEN DATE_DIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date, DAY) BETWEEN 4 AND 7 THEN 'Standard Transit'
    WHEN DATE_DIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date, DAY) > 7 THEN 'Slow Transit'
    ELSE 'Unknown Transit'
  END as carrier_leg_performance,

  -- Status de entrega
  CASE
    WHEN o.order_delivered_customer_date IS NOT NULL THEN 'Delivered'
    WHEN o.order_delivered_carrier_date IS NOT NULL THEN 'In Transit'
    WHEN o.order_approved_at IS NOT NULL THEN 'Processing'
    ELSE 'Pending'
  END as delivery_status,

  -- Classificação de frete
  CASE
    WHEN o.order_freight_value = 0 THEN 'Free Shipping'
    WHEN o.order_freight_value < 20 THEN 'Low Cost Shipping'
    WHEN o.order_freight_value BETWEEN 20 AND 50 THEN 'Medium Cost Shipping'
    WHEN o.order_freight_value > 50 THEN 'High Cost Shipping'
    ELSE 'Unknown'
  END as freight_category,

  -- Performance de entrega
  CASE
    WHEN DATE_DIFF(o.order_delivered_customer_date, o.order_approved_at, DAY) <= 7 THEN 'Fast Delivery'
    WHEN DATE_DIFF(o.order_delivered_customer_date, o.order_approved_at, DAY) BETWEEN 8 AND 15 THEN 'Standard Delivery'
    WHEN DATE_DIFF(o.order_delivered_customer_date, o.order_approved_at, DAY) > 15 THEN 'Slow Delivery'
    ELSE 'Pending Delivery'
  END as delivery_performance,

  -- Estatísticas comparativas da região/categoria
  stats.avg_days_to_carrier as region_category_avg_days_to_carrier,
  stats.avg_days_in_transit as region_category_avg_days_in_transit,
  stats.avg_total_delivery_days as region_category_avg_total_delivery_days,
  stats.total_orders as region_category_total_orders,
  SAFE_DIVIDE(stats.fast_deliveries, stats.total_orders) * 100 as region_category_fast_delivery_rate,
  SAFE_DIVIDE(stats.slow_deliveries, stats.total_orders) * 100 as region_category_slow_delivery_rate

FROM `olist-data-pipeline`.`intermediate`.`int_order_enriched` o
LEFT JOIN `olist-data-pipeline`.`staging`.`stg_sellers` s
  ON o.seller_id = s.seller_id
LEFT JOIN `olist-data-pipeline`.`staging`.`stg_customers` c
  ON o.customer_id = c.customer_id
LEFT JOIN delivery_stats_by_region_category stats ON (
  CASE
    WHEN s.seller_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
    WHEN s.seller_state IN ('RS', 'SC', 'PR') THEN 'Sul'
    WHEN s.seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN s.seller_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
    WHEN s.seller_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
    ELSE 'Outros'
  END = stats.seller_region
  AND CASE
    WHEN c.customer_state IN ('SP', 'RJ', 'ES', 'MG') THEN 'Sudeste'
    WHEN c.customer_state IN ('RS', 'SC', 'PR') THEN 'Sul'
    WHEN c.customer_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Centro-Oeste'
    WHEN c.customer_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
    WHEN c.customer_state IN ('AM', 'AC', 'RO', 'RR', 'AP', 'PA', 'TO') THEN 'Norte'
    ELSE 'Outros'
  END = stats.customer_region
  AND o.item_category = stats.item_category
)
WHERE o.order_approved_at IS NOT NULL
    );
  