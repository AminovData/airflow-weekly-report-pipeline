

WITH deduplicated_order_items AS (
  SELECT
    DATE_TRUNC(purchase_date, WEEK(MONDAY)) AS week_start_date,
    order_id,
    product_type,
    product_id,
    MAX(quantity) AS quantity,
    MAX(unit_price) AS unit_price
  FROM `dashboardprojectelsales.dashboard_dataset_final_dataset.daily_sales_with_tag_customers`
  GROUP BY week_start_date, order_id, product_type, product_id
),

aggregated_product_metrics AS (
  SELECT
    week_start_date,
    product_type,
    SUM(quantity) AS total_quantity_sold,
    SUM(quantity * unit_price) AS total_revenue
  FROM deduplicated_order_items
  GROUP BY week_start_date, product_type
)

SELECT
  week_start_date,
  product_type AS product_Type,
  total_quantity_sold AS total_quantity_sold,
  total_revenue AS Total_Revenue,
  RANK() OVER (PARTITION BY week_start_date ORDER BY total_quantity_sold DESC) AS product_rank
FROM aggregated_product_metrics
ORDER BY week_start_date, product_rank