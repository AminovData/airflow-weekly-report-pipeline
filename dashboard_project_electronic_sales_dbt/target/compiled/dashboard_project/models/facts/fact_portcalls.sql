
select
    year,
    country_code,
    commercial_ship_code,
    portcalls_count,
    median_port_time_days,
    avg_vessel_capacity_dwt,
    avg_container_capacity_teu
from
    `dashboardprojectelsales.staging_dataset.stg_cleaned_portcalls_merged`