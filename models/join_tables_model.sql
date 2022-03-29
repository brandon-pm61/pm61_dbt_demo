{{config(
  schema = "DATA_PIPELINE",
  materialized = "view"
)}}
WITH dbt_master_plan_table_wdates as (
  SELECT
    "ASP" AS "ASP",
    "DISC" AS "DISC",
    "ENDDATE" AS "ENDDATE",
    "MER" AS "REVENUE",
    "PLAN_ID" AS "PLAN_ID",
    "PLAN_NAME" AS "PLAN_NAME",
    "LEN" AS "LEN",
    "PACKAGE" AS "PACKAGE",
    "STARTDATE" AS "STARTDATE",
    "ROLLUP1" AS "ROLLUP1"
  FROM
    {{ ref('dbt_master_plan_table_wdates') }}
),
dbt_fact_subscription_activity as (
    SELECT 
    "FROM_PLAN_ID" AS "FROM_PLAN_ID",
    "FROM_PLAN_KEY" AS "FROM_PLAN_KEY",
    "SBSCRN_ID" AS "SBSCRN_ID",
    "DEACTIVATION_REASON_CODE" AS "DEACTIVATION_REASON_CODE",
    "EQMNT_KEY" AS "EQMNT_KEY",
    "VEH_KEY" AS "VEH_KEY",
    "CALL_DISPOSITION" AS "CALL_DISPOSITION",
    "DVC_ID" AS "DVC_ID",
    "PARNT_ACCT_NUM" AS "PARNT_ACCT_NUM",
    "PKG_ID" AS "PKG_ID",
    "ACTVTY_TS" AS "ACTVTY_TS",
    "EQMNT_ID" AS "EQMNT_ID",
    "PROD_KEY" AS "PROD_KEY",
    "DVC_KEY" AS "DVC_KEY",
    "CALL_REASON" AS "CALL_REASON",
    "PLAN_ID" AS "PLAN_ID1",
    "ACTVTY_TYPE_ID" AS "ACTVTY_TYPE_ID",
    "ACTVTY_DT" AS "ACTVTY_DT",
    "AGN_KEY" AS "AGN_KEY",
    "BILLING_METHOD" AS "BILLING_METHOD",
    "PLAN_KEY" AS "PLAN_KEY",
    "PROD_ID" AS "PROD_ID"
  FROM
     {{ ref('dbt_fact_subscription_activity') }} 
),
join_step2 as (
  SELECT
    dbt_master_plan_table_wdates."ASP" AS "ASP",
    dbt_master_plan_table_wdates."DISC" AS "DISC",
    dbt_master_plan_table_wdates."ENDDATE" AS "ENDDATE",
    dbt_master_plan_table_wdates."REVENUE" AS "REVENUE",
    dbt_master_plan_table_wdates."PLAN_ID" AS "PLAN_ID",
    dbt_master_plan_table_wdates."PLAN_NAME" AS "PLAN_NAME",
    dbt_master_plan_table_wdates."LEN" AS "LEN",
    dbt_master_plan_table_wdates."PACKAGE" AS "PACKAGE",
    dbt_master_plan_table_wdates."STARTDATE" AS "STARTDATE",
    dbt_master_plan_table_wdates."ROLLUP1" AS "ROLLUP1",
    dbt_fact_subscription_activity."FROM_PLAN_ID" AS "FROM_PLAN_ID",
    dbt_fact_subscription_activity."FROM_PLAN_KEY" AS "FROM_PLAN_KEY",
    dbt_fact_subscription_activity."SBSCRN_ID" AS "SBSCRN_ID",
    dbt_fact_subscription_activity."DEACTIVATION_REASON_CODE" AS "DEACTIVATION_REASON_CODE",
    dbt_fact_subscription_activity."EQMNT_KEY" AS "EQMNT_KEY",
    dbt_fact_subscription_activity."VEH_KEY" AS "VEH_KEY",
    dbt_fact_subscription_activity."CALL_DISPOSITION" AS "CALL_DISPOSITION",
    dbt_fact_subscription_activity."DVC_ID" AS "DVC_ID",
    dbt_fact_subscription_activity."PARNT_ACCT_NUM" AS "PARNT_ACCT_NUM",
    dbt_fact_subscription_activity."PKG_ID" AS "PKG_ID",
    dbt_fact_subscription_activity."ACTVTY_TS" AS "ACTVTY_TS",
    dbt_fact_subscription_activity."EQMNT_ID" AS "EQMNT_ID",
    dbt_fact_subscription_activity."PROD_KEY" AS "PROD_KEY",
    dbt_fact_subscription_activity."DVC_KEY" AS "DVC_KEY",
    dbt_fact_subscription_activity."CALL_REASON" AS "CALL_REASON",
    dbt_fact_subscription_activity."PLAN_ID1" AS "PLAN_ID1",
    dbt_fact_subscription_activity."ACTVTY_TYPE_ID" AS "ACTVTY_TYPE_ID",
    dbt_fact_subscription_activity."ACTVTY_DT" AS "ACTVTY_DT",
    dbt_fact_subscription_activity."AGN_KEY" AS "AGN_KEY",
    dbt_fact_subscription_activity."BILLING_METHOD" AS "BILLING_METHOD",
    dbt_fact_subscription_activity."PLAN_KEY" AS "PLAN_KEY",
    dbt_fact_subscription_activity."PROD_ID" AS "PROD_ID"
  FROM
    dbt_master_plan_table_wdates
    LEFT OUTER JOIN dbt_fact_subscription_activity ON (
      dbt_master_plan_table_wdates."PLAN_ID" = dbt_fact_subscription_activity."PLAN_ID1"
    )
),
filter_step3 as (
  SELECT
    *
  FROM
    join_step2
  WHERE
    (join_step2."DISC" <> 'TBD')
)
SELECT
  *
FROM
  filter_step3
LIMIT
  1000
