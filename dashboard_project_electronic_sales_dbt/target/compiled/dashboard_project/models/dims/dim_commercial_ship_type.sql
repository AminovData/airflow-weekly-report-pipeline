

select distinct
    commercial_ship_code,
    commercial_ship_type
from
    `dashboardprojectelsales.staging_dataset.stg_cleaned_portcalls_merged`
where commercial_ship_code is not null