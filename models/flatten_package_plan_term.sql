{{config(
  schema = "DATA_PIPELINE",
  materialized = "table"
)}}
{% set term_query %}
select distinct
regexp_replace("LEN", ' ', '_') AS term
from "PROMETHIUM"."SCHEMA_INFO"."MASTER_PLAN_TABLE_WDATES"
order by 1
{% endset %}

{% set results = run_query(term_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{%- set lens = ["Annual", "Semi Annual", "Monthly", "Two Year", "Quarterly", "5 Months", "3 Year", "5 Year", "Lifetime", "2 Months", "4 Months", "TBD"] -%}

WITH AGENT_DEMOGRAPHICS as (
  SELECT
    "PKG_ID" AS "PKG_ID",
    "FROM_PKG_ID" AS "FROM_PKG_ID",
    "MRD_TYPE_CD" AS "MRD_TYPE_CD",
    "BILLING_METHOD" AS "BILLING_METHOD",
    "AGN_KEY" AS "AGN_KEY",
    "ACTIVATION_SOURCE" AS "ACTIVATION_SOURCE",
    "VENDOR" AS "VENDOR",
    "STATUS" AS "STATUS",
    "SITE" AS "SITE"
  FROM
    {{ ref('AGENT_DEMOGRAPHICS_MQT_DBT') }}
)
select
"PACKAGE", "PLAN_NAME", 
{%- for len in lens %}
sum(case when len = '{{len}}' then revenue end) 
as TERM_{{dbt_utils.slugify(len)}}
{%- if not loop.last %},{% endif -%}
{% endfor %}
from {{ ref('joinfilter_offers_cancel_reasons') }}, AGENT_DEMOGRAPHICS
where "AGN_KEY" = AGENT_DEMOGRAPHICS."AGN_KEY"
group by 1, 2