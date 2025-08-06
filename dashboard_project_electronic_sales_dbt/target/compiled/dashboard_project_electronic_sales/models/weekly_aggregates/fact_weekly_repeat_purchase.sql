

WITH unique_order_products AS (
  SELECT
    DATE_TRUNC(purchase_date, WEEK(MONDAY)) AS week_start_date,
    order_id,
    product_type
  FROM
    `your_dataset.your_table`
  GROUP BY
    week_start_date, order_id, product_type
),

weekly_product_counts AS (
  SELECT
    week_start_date,
    product_type,
    COUNT(DISTINCT order_id) AS order_count  -- number of orders containing this product_type in that week
  FROM
    unique_order_products
  GROUP BY
    week_start_date, product_type
),

ranked_products AS (
  SELECT
    week_start_date,
    product_type,
    order_count,
    RANK() OVER (PARTITION BY week_start_date ORDER BY order_count DESC) AS product_rank
  FROM
    weekly_product_counts
)

SELECT
  week_start_date,
  product_type,
  order_count,
  product_rank
FROM
  ranked_products
WHERE
  product_rank <= 10
ORDER BY
  week_start_date,
  product_rank