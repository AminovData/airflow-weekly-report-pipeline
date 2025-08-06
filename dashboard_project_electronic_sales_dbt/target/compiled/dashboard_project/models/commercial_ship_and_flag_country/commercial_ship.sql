

with merged_portcalls as (

  select
    f.year,
    f.country_code,
    dc.country,
    f.commercial_ship_code,
    dcs.commercial_ship_type,
    f.portcalls_count,
    f.median_port_time_days,
    f.avg_vessel_capacity_dwt,
    f.avg_container_capacity_teu

  from `dashboardprojectelsales.dashboard_dataset_facts.fact_portcalls` f
  left join `dashboardprojectelsales.dashboard_dataset_dims.dim_country` dc
    on f.country_code = dc.country_code
  left join `dashboardprojectelsales.dashboard_dataset_dims.dim_commercial_ship_type` dcs
    on f.commercial_ship_code = dcs.commercial_ship_code

)

select * from merged_portcalls