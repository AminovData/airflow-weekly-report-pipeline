

WITH order_level AS (
    SELECT
        order_id,
        customer_id,
        DATE_TRUNC(purchase_date, WEEK(MONDAY)) AS week_start_date,

        MAX(total_price) + MAX(addon_total) AS order_total_price,  -- ✅ FIXED to include addon only once
        SUM(quantity) AS order_total_quantity,
        COUNT(DISTINCT product_type) AS distinct_product_types_per_order,
        MAX(CAST(is_returning_customer AS INT64)) AS is_returning

    FROM `dashboardprojectelsales.dashboard_dataset_final_dataset.daily_sales_with_tag_customers`
    GROUP BY order_id, customer_id, week_start_date
),

weekly_order_aggregates AS (
    SELECT
        week_start_date,
        SUM(order_total_price) AS total_sales,
        COUNT(order_id) AS total_orders,
        AVG(order_total_quantity) AS avg_quantity_per_order,
        AVG(distinct_product_types_per_order) AS avg_distinct_product_types
    FROM order_level
    GROUP BY week_start_date
),

weekly_customers AS (
    SELECT
        week_start_date,
        COUNT(DISTINCT customer_id) AS total_customers,
        COUNTIF(is_returning = 1) AS returning_customers,
        SAFE_DIVIDE(COUNTIF(is_returning = 1), COUNT(DISTINCT customer_id)) AS repeat_purchase_rate
    FROM order_level
    GROUP BY week_start_date
)

SELECT
    w.week_start_date,
    w.total_sales,
    w.total_orders,
    c.total_customers,
    c.returning_customers,
    c.repeat_purchase_rate,
    w.avg_quantity_per_order,
    w.avg_distinct_product_types
FROM
    weekly_order_aggregates w
LEFT JOIN
    weekly_customers c USING (week_start_date)
ORDER BY
    w.week_start_date