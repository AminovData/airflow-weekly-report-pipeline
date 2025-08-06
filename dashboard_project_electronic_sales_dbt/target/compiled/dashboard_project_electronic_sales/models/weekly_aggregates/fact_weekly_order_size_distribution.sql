

WITH order_sizes AS (
    SELECT
        order_id,
        DATE_TRUNC(purchase_date, WEEK(MONDAY)) AS order_week,
        SUM(quantity) AS total_quantity_per_order,
        COUNT(DISTINCT product_type) AS distinct_products_per_order
    FROM
        `dashboardprojectelsales.dashboard_dataset.daily_electronic_sales`
    GROUP BY
        order_id, order_week
)

SELECT
    order_week,
    AVG(total_quantity_per_order) AS avg_quantity_per_order,
    AVG(distinct_products_per_order) AS avg_distinct_products_per_order
FROM
    order_sizes
GROUP BY
    order_week
ORDER BY
    order_week