

WITH labeled_orders AS (
    SELECT
        *,
        DATE_TRUNC(purchase_date, WEEK(MONDAY)) AS order_week,
        MIN(purchase_date) OVER (PARTITION BY customer_id) AS first_order_date
    FROM
        `dashboardprojectelsales.dashboard_dataset.daily_electronic_sales`
)

SELECT
    *,
    CASE 
        WHEN first_order_date < order_week THEN TRUE
        ELSE FALSE
    END AS is_returning_customer
FROM
    labeled_orders