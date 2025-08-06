

select
  year,
  country_code,
  ship_type_code,
  dwt_thousands,
  world_percentage,
  fleet_percentage,
  ship_count,
  gt_thousands,
  avg_vessel_age_years
from `dashboardprojectelsales.staging_dataset.stg_cleaned_merchant_fleet`