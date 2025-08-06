CREATE OR REPLACE TABLE `dashboardprojectelsales.dashboard_dataset.agg_fleet_ownership` AS
        SELECT
            year,
            country_code,
            country,
            SUM(dwt_thousands) AS dwt_thousands,
            SUM(fleet_percentage) AS fleet_percentage,
            SUM(ship_count) AS ship_count
        FROM `dashboardprojectelsales.dashboard_dataset.stg_clean_fleet_ownership`
        GROUP BY year, country_code, country