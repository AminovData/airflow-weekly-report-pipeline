

WITH weighted_age AS (
    SELECT
        year,
        country_code,
        country,
        SUM(avg_vessel_age_years * ship_count) / NULLIF(SUM(ship_count), 0) AS avg_vessel_age_years
    FROM `dashboardprojectelsales.dashboard_dataset.stg_cleaned_merchant_fleet`
    GROUP BY year, country_code, country
)

SELECT
    f.year,
    f.country_code,
    f.country,
    SUM(dwt_thousands) AS dwt_thousands,
    SUM(gt_thousands) AS gt_thousands,
    SUM(ship_count) AS ship_count,
    SUM(fleet_percentage) AS fleet_percentage,
    SUM(world_percentage) AS world_percentage,
    w.avg_vessel_age_years
FROM `dashboardprojectelsales.dashboard_dataset.stg_cleaned_merchant_fleet` f
LEFT JOIN weighted_age w
    ON f.year = w.year AND f.country_code = w.country_code AND f.country = w.country
GROUP BY f.year, f.country_code, f.country, w.avg_vessel_age_years