

with

dim_country as (
  select distinct country_code, country
  from `dashboardprojectelsales.dashboard_dataset_dims.dim_country`
  where country_code is not null
),

lsci_agg as (
  with latest_q4_yoy as (
    select year, country_code, growth_rate_yoy
    from `dashboardprojectelsales.dashboard_dataset_facts.fact_lsci`
    where quarter = 'Q4'
  )
  select
    a.year,
    a.country_code,
    dc.country,
    avg(lsci_index) as lsci_index,
    avg(growth_rate_qoq) as growth_rate_qoq,
    avg(lsci_rank) as lsci_rank,
    b.growth_rate_yoy
  from `dashboardprojectelsales.dashboard_dataset_facts.fact_lsci` a
  left join latest_q4_yoy b on a.year = b.year and a.country_code = b.country_code
  left join dim_country dc on a.country_code = dc.country_code
  group by a.year, a.country_code, b.growth_rate_yoy, dc.country
),

fleet_ownership_agg as (
  select
    f.year,
    f.country_code,
    dc.country,
    sum(dwt_thousands) as dwt_thousands_ownership,
    sum(ship_count) as ship_count_ownership
  from `dashboardprojectelsales.dashboard_dataset_facts.fact_fleet_ownership` f
  left join dim_country dc on f.country_code = dc.country_code
  group by f.year, f.country_code, dc.country
),

merchant_fleet_agg as (
  with weighted_age as (
    select
      year,
      country_code,
      sum(avg_vessel_age_years * ship_count) / nullif(sum(ship_count), 0) as avg_vessel_age_years
    from `dashboardprojectelsales.dashboard_dataset_facts.fact_fleet`
    group by year, country_code
  )
  select
    f.year,
    f.country_code,
    dc.country,
    sum(dwt_thousands) as dwt_thousands,
    sum(gt_thousands) as gt_thousands,
    sum(ship_count) as ship_count,
    sum(fleet_percentage) as fleet_percentage,
    sum(world_percentage) as world_percentage,
    w.avg_vessel_age_years
  from `dashboardprojectelsales.dashboard_dataset_facts.fact_fleet` f
  left join weighted_age w on f.year = w.year and f.country_code = w.country_code
  left join dim_country dc on f.country_code = dc.country_code
  group by f.year, f.country_code, dc.country, w.avg_vessel_age_years
),

portcalls_agg as (
  with portcall_sums as (
    select year, country_code, sum(portcalls_count) as portcalls_count
    from `dashboardprojectelsales.dashboard_dataset_facts.fact_portcalls`
    group by year, country_code
  ),  -- <--- comma added here
  weighted_values as (
    select
      year,
      country_code,
      sum(median_port_time_days * portcalls_count) / nullif(sum(portcalls_count), 0) as median_port_time_days,
      sum(avg_vessel_capacity_dwt * portcalls_count) / nullif(sum(portcalls_count), 0) as avg_vessel_capacity_dwt,
      sum(avg_container_capacity_teu * portcalls_count) / nullif(sum(portcalls_count), 0) as avg_container_capacity_teu
    from `dashboardprojectelsales.dashboard_dataset_facts.fact_portcalls`
    group by year, country_code
  )
  select
    s.year,
    s.country_code,
    dc.country,
    s.portcalls_count,
    w.median_port_time_days,
    w.avg_vessel_capacity_dwt,
    w.avg_container_capacity_teu
  from portcall_sums s
  join weighted_values w on s.year = w.year and s.country_code = w.country_code
  left join dim_country dc on s.country_code = dc.country_code
)

select
  coalesce(l.year, f.year, m.year, p.year) as year,
  coalesce(l.country_code, f.country_code, m.country_code, p.country_code) as country_code,
  coalesce(l.country, f.country, m.country, p.country) as country,

  -- LSCI
  l.lsci_index,
  l.growth_rate_qoq,
  l.lsci_rank,
  l.growth_rate_yoy,

  -- Fleet ownership
  f.dwt_thousands_ownership,
  f.ship_count_ownership,

  -- Merchant fleet
  m.dwt_thousands,
  m.gt_thousands,
  m.ship_count,
  m.fleet_percentage,
  m.world_percentage,
  m.avg_vessel_age_years,

  -- Portcalls
  p.portcalls_count,
  p.median_port_time_days,
  p.avg_vessel_capacity_dwt,
  p.avg_container_capacity_teu

from lsci_agg l
full outer join fleet_ownership_agg f on l.year = f.year and l.country_code = f.country_code
full outer join merchant_fleet_agg m on coalesce(l.year, f.year) = m.year and coalesce(l.country_code, f.country_code) = m.country_code
full outer join portcalls_agg p on coalesce(l.year, f.year, m.year) = p.year and coalesce(l.country_code, f.country_code, m.country_code) = p.country_code