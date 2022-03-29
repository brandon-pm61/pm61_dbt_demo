{{config(
	schema = "DATA_PIPELINE",
	materialized = "view"
)}}
WITH select_step1 as (
  SELECT
    "dbt_master_plan_table_wdates"."ASP" AS "ASP",
    "dbt_master_plan_table_wdates"."DISC" AS "DISC",
    "dbt_master_plan_table_wdates"."ENDDATE" AS "ENDDATE",
    "dbt_master_plan_table_wdates"."MER" AS "MER",
    "dbt_master_plan_table_wdates"."PLAN_ID" AS "PLAN_ID",
    "dbt_master_plan_table_wdates"."PLAN_NAME" AS "PLAN_NAME",
    "dbt_master_plan_table_wdates"."LEN" AS "LEN",
    "dbt_master_plan_table_wdates"."PACKAGE" AS "PACKAGE",
    "dbt_master_plan_table_wdates"."STARTDATE" AS "STARTDATE",
    "dbt_master_plan_table_wdates"."ROLLUP1" AS "ROLLUP1"
  FROM
    "DATA_PIPELINE"."dbt_master_plan_table_wdates"
),
join_step2 as (
  SELECT
    select_step1."ASP" AS "ASP",
    select_step1."DISC" AS "DISC",
    select_step1."ENDDATE" AS "ENDDATE",
    select_step1."MER" AS "MER",
    select_step1."PLAN_ID" AS "PLAN_ID",
    select_step1."PLAN_NAME" AS "PLAN_NAME",
    select_step1."LEN" AS "LEN",
    select_step1."PACKAGE" AS "PACKAGE",
    select_step1."STARTDATE" AS "STARTDATE",
    select_step1."ROLLUP1" AS "ROLLUP1",
    "dbt_fact_subscription_activity"."FROM_PLAN_ID" AS "FROM_PLAN_ID",
    "dbt_fact_subscription_activity"."FROM_PLAN_KEY" AS "FROM_PLAN_KEY",
    "dbt_fact_subscription_activity"."SBSCRN_ID" AS "SBSCRN_ID",
    "dbt_fact_subscription_activity"."DEACTIVATION_REASON_CODE" AS "DEACTIVATION_REASON_CODE",
    "dbt_fact_subscription_activity"."EQMNT_KEY" AS "EQMNT_KEY",
    "dbt_fact_subscription_activity"."VEH_KEY" AS "VEH_KEY",
    "dbt_fact_subscription_activity"."CALL_DISPOSITION" AS "CALL_DISPOSITION",
    "dbt_fact_subscription_activity"."DVC_ID" AS "DVC_ID",
    "dbt_fact_subscription_activity"."PARNT_ACCT_NUM" AS "PARNT_ACCT_NUM",
    "dbt_fact_subscription_activity"."PKG_ID" AS "PKG_ID",
    "dbt_fact_subscription_activity"."ACTVTY_TS" AS "ACTVTY_TS",
    "dbt_fact_subscription_activity"."EQMNT_ID" AS "EQMNT_ID",
    "dbt_fact_subscription_activity"."PROD_KEY" AS "PROD_KEY",
    "dbt_fact_subscription_activity"."DVC_KEY" AS "DVC_KEY",
    "dbt_fact_subscription_activity"."CALL_REASON" AS "CALL_REASON",
    "dbt_fact_subscription_activity"."PLAN_ID" AS "PLAN_ID1",
    "dbt_fact_subscription_activity"."ACTVTY_TYPE_ID" AS "ACTVTY_TYPE_ID",
    "dbt_fact_subscription_activity"."ACTVTY_DT" AS "ACTVTY_DT",
    "dbt_fact_subscription_activity"."AGN_KEY" AS "AGN_KEY",
    "dbt_fact_subscription_activity"."BILLING_METHOD" AS "BILLING_METHOD",
    "dbt_fact_subscription_activity"."PLAN_KEY" AS "PLAN_KEY",
    "dbt_fact_subscription_activity"."PROD_ID" AS "PROD_ID"
  FROM
    select_step1
    LEFT OUTER JOIN "DATA_PIPELINE"."dbt_fact_subscription_activity" ON (
      select_step1."PLAN_ID" = "dbt_fact_subscription_activity"."PLAN_ID"
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