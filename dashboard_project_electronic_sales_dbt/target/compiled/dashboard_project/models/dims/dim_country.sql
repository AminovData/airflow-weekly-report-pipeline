

select distinct
    country_code,
    country
from
    `dashboardprojectelsales.staging_dataset.stg_clean_fleet_ownership`
where country_code is not null