CREATE OR REPLACE TABLE `dashboardprojectelsales.dashboard_dataset.stg_cleaned_lsci` AS
        WITH latest_q4_yoy AS (
            SELECT year, country_code, growth_rate_yoy
            FROM `dashboardprojectelsales.dashboard_dataset.stg_cleaned_lsci`
            WHERE quarter = 'Q4'
        )
        SELECT
            a.year,
            a.country_code,
            a.country,
            AVG(lsci_index) AS lsci_index,
            AVG(growth_rate_qoq) AS growth_rate_qoq,
            AVG(lsci_rank) AS lsci_rank,
            b.growth_rate_yoy
        FROM `dashboardprojectelsales.dashboard_dataset.stg_cleaned_lsci` a
        LEFT JOIN latest_q4_yoy b
        ON a.year = b.year AND a.country_code = b.country_code
        GROUP BY a.year, a.country_code, a.country, b.growth_rate_yoy