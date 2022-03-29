{{config(
  schema = "DATA_PIPELINE",
  materialized = "view"
)}}
select
  package, plan_name,
  {{ dbt_utils.pivot(
      'len',
      dbt_utils.get_column_values(ref('joinfilter_offers_cancel_reasons'), 'len')
  ) }}
from {{ ref('joinfilter_offers_cancel_reasons') }}
group by package, plan_name