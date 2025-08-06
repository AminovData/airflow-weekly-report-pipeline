{{ config(materialized='table') }}

WITH addon_base AS (
    SELECT
        DATE_TRUNC(purchase_date, WEEK(MONDAY)) AS week_start_date,
        addon_purchased
    FROM `dashboardprojectelsales.dashboard_dataset_final_dataset.daily_sales_with_tag_customers`
    WHERE addon_purchased IS NOT NULL
),

addon_agg AS (
    SELECT
        week_start_date,
        addon_purchased,
        COUNT(*) AS total_addon_count
    FROM addon_base
    GROUP BY week_start_date, addon_purchased
)

SELECT
    week_start_date,
    CASE 
        WHEN addon_purchased = 'none' THEN 'not purchased'
        ELSE addon_purchased
    END AS additional_item,
    total_addon_count AS quantity,
    RANK() OVER (PARTITION BY week_start_date ORDER BY total_addon_count DESC) AS rank
FROM addon_agg
ORDER BY week_start_date, rank
