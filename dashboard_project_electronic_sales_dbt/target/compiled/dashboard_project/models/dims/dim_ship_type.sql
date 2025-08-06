
select distinct
    ship_type_code,
    ship_type
from `dashboardprojectelsales.staging_dataset.stg_cleaned_merchant_fleet`
where ship_type_code is not null