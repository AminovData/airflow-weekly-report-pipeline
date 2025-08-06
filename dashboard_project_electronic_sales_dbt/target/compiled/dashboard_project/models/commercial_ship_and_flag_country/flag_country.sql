

with merged_fleet_ownership as (

  select
    f.year,
    f.flag_code,
    df.flag_country,
    f.country_code,
    dc.country,
    f.dwt_thousands,
    f.fleet_percentage,
    f.ship_count

  from `dashboardprojectelsales.dashboard_dataset_facts.fact_fleet_ownership` f
  left join `dashboardprojectelsales.dashboard_dataset_dims.dim_flag` df
    on f.flag_code = df.flag_code
  left join `dashboardprojectelsales.dashboard_dataset_dims.dim_country` dc
    on f.country_code = dc.country_code

)

select * from merged_fleet_ownership