

select
    year,
    quarter,
    country_code,
    lsci_index,
    growth_rate_yoy,
    growth_rate_qoq,
    lsci_rank
from
   `dashboardprojectelsales.staging_dataset.stg_cleaned_lsci`