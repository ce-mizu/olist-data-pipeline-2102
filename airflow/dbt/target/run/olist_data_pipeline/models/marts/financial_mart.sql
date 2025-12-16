
  
    

    create or replace table `olist-data-pipeline`.`marts`.`financial_mart`
      
    
    

    OPTIONS()
    as (
      

-- Mart Financeiro: Análises de pagamentos, receitas e share de faturamento
WITH payment_method_revenue AS (
  SELECT
    payment_type,
    SUM(payment_value) as total_revenue_by_method,
    COUNT(*) as total_transactions_by_method,
    AVG(payment_value) as avg_transaction_value
  FROM `olist-data-pipeline`.`intermediate`.`int_payment_enriched` p
  WHERE order_approved_at IS NOT NULL
  GROUP BY payment_type
),

total_revenue_calc AS (
  SELECT
    SUM(payment_value) as grand_total_revenue
  FROM `olist-data-pipeline`.`intermediate`.`int_payment_enriched` p
  WHERE order_approved_at IS NOT NULL
),

payment_share_analysis AS (
  SELECT
    pmr.*,
    tr.grand_total_revenue,
    (pmr.total_revenue_by_method / tr.grand_total_revenue) * 100 as revenue_share_percentage
  FROM payment_method_revenue pmr
  CROSS JOIN total_revenue_calc tr
),

monthly_revenue_billing AS (
  SELECT
    EXTRACT(YEAR FROM order_approved_at) as billing_year,
    EXTRACT(MONTH FROM order_approved_at) as billing_month,
    payment_type,
    SUM(payment_value) as monthly_billing
  FROM `olist-data-pipeline`.`intermediate`.`int_payment_enriched` p
  WHERE order_approved_at IS NOT NULL
  GROUP BY EXTRACT(YEAR FROM order_approved_at), EXTRACT(MONTH FROM order_approved_at), payment_type
),

cumulative_billing AS (
  SELECT
    *,
    SUM(monthly_billing) OVER (
      PARTITION BY payment_type
      ORDER BY billing_year, billing_month
      ROWS UNBOUNDED PRECEDING
    ) as cumulative_billing
  FROM monthly_revenue_billing
),

monthly_revenue_receipt AS (
  SELECT
    EXTRACT(YEAR FROM receipt_date) as receipt_year,
    EXTRACT(MONTH FROM receipt_date) as receipt_month,
    payment_type,
    SUM(payment_value) as monthly_receipt
  FROM `olist-data-pipeline`.`intermediate`.`int_payment_enriched` p
  WHERE receipt_date IS NOT NULL
  GROUP BY EXTRACT(YEAR FROM receipt_date), EXTRACT(MONTH FROM receipt_date), payment_type
),

cumulative_receipt AS (
  SELECT
    *,
    SUM(monthly_receipt) OVER (
      PARTITION BY payment_type
      ORDER BY receipt_year, receipt_month
      ROWS UNBOUNDED PRECEDING
    ) as cumulative_receipt
  FROM monthly_revenue_receipt
)

SELECT
  p.order_id,
  p.payment_sequential,
  p.payment_type,
  p.payment_installments,
  p.payment_value,
  p.order_approved_at,
  p.receipt_date,

  -- Informações de share de faturamento por meio de pagamento
  psa.total_revenue_by_method,
  psa.total_transactions_by_method,
  psa.avg_transaction_value,
  psa.revenue_share_percentage,
  psa.grand_total_revenue,

  -- Cálculos financeiros
  DATE_DIFF(p.receipt_date, p.order_approved_at, DAY) as days_to_receipt,

  -- Valor por parcela
  CASE
    WHEN p.payment_installments > 0 THEN p.payment_value / p.payment_installments
    ELSE p.payment_value
  END as installment_value,

  -- Classificação de pagamento
  CASE
    WHEN p.payment_type = 'credit_card' THEN 'Credit Payment'
    WHEN p.payment_type = 'debit_card' THEN 'Debit Payment'
    WHEN p.payment_type = 'boleto' THEN 'Bank Slip'
    WHEN p.payment_type = 'voucher' THEN 'Voucher Payment'
    ELSE 'Other Payment'
  END as payment_category,

  -- Classificação por valor
  CASE
    WHEN p.payment_value < 50 THEN 'Small Transaction'
    WHEN p.payment_value BETWEEN 50 AND 200 THEN 'Medium Transaction'
    WHEN p.payment_value BETWEEN 200 AND 500 THEN 'Large Transaction'
    WHEN p.payment_value > 500 THEN 'Premium Transaction'
    ELSE 'Unknown'
  END as transaction_size,

  -- Análise de parcelamento
  CASE
    WHEN p.payment_installments = 1 THEN 'Single Payment'
    WHEN p.payment_installments BETWEEN 2 AND 6 THEN 'Short Term Installment'
    WHEN p.payment_installments BETWEEN 7 AND 12 THEN 'Medium Term Installment'
    WHEN p.payment_installments > 12 THEN 'Long Term Installment'
    ELSE 'Unknown Installment'
  END as installment_category,

  -- Dimensões temporais
  EXTRACT(YEAR FROM p.order_approved_at) as payment_year,
  EXTRACT(MONTH FROM p.order_approved_at) as payment_month,
  FORMAT_DATE('%A', p.order_approved_at) as payment_day_name,

  -- Curvas de faturamento vs recebimento (billing vs receipt)
  mrb.monthly_billing,
  mrb.cumulative_billing,
  mrr.monthly_receipt,
  mrr.cumulative_receipt,

  -- Análise de gap entre faturamento e recebimento
  COALESCE(mrr.monthly_receipt, 0) - COALESCE(mrb.monthly_billing, 0) as monthly_billing_receipt_gap,
  COALESCE(mrr.cumulative_receipt, 0) - COALESCE(mrb.cumulative_billing, 0) as cumulative_billing_receipt_gap,

  -- Indicadores de eficiência de recebimento
  CASE
    WHEN mrb.monthly_billing > 0 THEN (COALESCE(mrr.monthly_receipt, 0) / mrb.monthly_billing) * 100
    ELSE NULL
  END as monthly_receipt_efficiency_percentage,

-- Faturamento acumulado individual por tipo de pagamento
  SUM(p.payment_value) OVER (
    PARTITION BY p.payment_type
    ORDER BY p.order_approved_at, p.order_id
    ROWS UNBOUNDED PRECEDING
  ) as individual_cumulative_billing,

  SUM(CASE WHEN p.receipt_date IS NOT NULL THEN p.payment_value ELSE 0 END) OVER (
    PARTITION BY p.payment_type
    ORDER BY COALESCE(p.receipt_date, p.order_approved_at), p.order_id
    ROWS UNBOUNDED PRECEDING
  ) as individual_cumulative_receipt

FROM `olist-data-pipeline`.`intermediate`.`int_payment_enriched` p
LEFT JOIN payment_share_analysis psa
  ON p.payment_type = psa.payment_type
LEFT JOIN cumulative_billing mrb ON (
  p.payment_type = mrb.payment_type
  AND EXTRACT(YEAR FROM p.order_approved_at) = mrb.billing_year
  AND EXTRACT(MONTH FROM p.order_approved_at) = mrb.billing_month
)
LEFT JOIN cumulative_receipt mrr ON (
  p.payment_type = mrr.payment_type
  AND EXTRACT(YEAR FROM p.receipt_date) = mrr.receipt_year
  AND EXTRACT(MONTH FROM p.receipt_date) = mrr.receipt_month
)
WHERE p.order_approved_at IS NOT NULL
    );
  