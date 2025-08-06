

WITH portcall_sums AS (
    SELECT
        year,
        country_code,
        country,
        SUM(portcalls_count) AS portcalls_count
    FROM `dashboardprojectelsales.dashboard_dataset.stg_cleaned_portcalls_merged`
    GROUP BY year, country_code, country
),
weighted_values AS (
    SELECT
        year,
        country_code,
        country,
        SUM(median_port_time_days * portcalls_count) / NULLIF(SUM(portcalls_count), 0) AS median_port_time_days,
        SUM(avg_vessel_capacity_dwt * portcalls_count) / NULLIF(SUM(portcalls_count), 0) AS avg_vessel_capacity_dwt,
        SUM(avg_container_capacity_teu * portcalls_count) / NULLIF(SUM(portcalls_count), 0) AS avg_container_capacity_teu
    FROM `dashboardprojectelsales.dashboard_dataset.stg_cleaned_portcalls_merged`
    GROUP BY year, country_code, country
)

SELECT
    s.year,
    s.country_code,
    s.country,
    s.portcalls_count,
    w.median_port_time_days,
    w.avg_vessel_capacity_dwt,
    w.avg_container_capacity_teu
FROM portcall_sums s
JOIN weighted_values w
    ON s.year = w.year
    AND s.country_code = w.country_code
    AND s.country = w.country