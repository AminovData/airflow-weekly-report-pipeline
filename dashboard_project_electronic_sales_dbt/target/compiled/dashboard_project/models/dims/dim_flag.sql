

select distinct
    flag_code,
    flag_country
from `dashboardprojectelsales.staging_dataset.stg_clean_fleet_ownership`
where flag_code is not null