{{config(
	catalog = "hive",
	schema = "promethium",
	materialized = "view"
)}}
WITH select_step1 as (
  SELECT
    "oracle"."CUSTOMER_DEMO"."MASTER_PLAN_TABLE_WDATES"."LEN" AS "LEN",
    "oracle"."CUSTOMER_DEMO"."MASTER_PLAN_TABLE_WDATES"."PLAN_NAME" AS "PLAN_NAME",
    "oracle"."CUSTOMER_DEMO"."MASTER_PLAN_TABLE_WDATES"."PACKAGE" AS "PACKAGE",
    "oracle"."CUSTOMER_DEMO"."MASTER_PLAN_TABLE_WDATES"."MER" AS "MER",
    "oracle"."CUSTOMER_DEMO"."MASTER_PLAN_TABLE_WDATES"."PLAN_ID" AS "PLAN_ID"
  FROM
    "oracle"."CUSTOMER_DEMO"."MASTER_PLAN_TABLE_WDATES"
),
join_step2 as (
  SELECT
    select_step1."LEN" AS "LEN",
    select_step1."PLAN_NAME" AS "PLAN_NAME",
    select_step1."PACKAGE" AS "PACKAGE",
    select_step1."MER" AS "MER",
    "postgresql"."sample_qa"."fact_subscription_activity"."SBSCRN_ID" AS "SBSCRN_ID",
    select_step1."PLAN_ID" AS "PLAN_ID"
  FROM
    select_step1
    LEFT OUTER JOIN "postgresql"."sample_qa"."fact_subscription_activity" ON (
      select_step1."PLAN_ID" = "postgresql"."sample_qa"."fact_subscription_activity"."PLAN_ID"
    )
),
group_by_step3 as (
  SELECT
    join_step2."LEN" AS "LEN",
    join_step2."PACKAGE" AS "PACKAGE",
    join_step2."PLAN_NAME" AS "PLAN_NAME",
    sum(MER) as "REVENUE"
  FROM
    join_step2
  GROUP BY
    join_step2."LEN",
    join_step2."PACKAGE",
    join_step2."PLAN_NAME"
)
{%- set lens = ["Annual", "Semi Annual", "Monthly", "Two Year", "Quarterly", "5 Months", "3 Year", "5 Year", "Lifetime", "2 Months", "4 Months", "TBD"] -%}

select

"PACKAGE", "PLAN_NAME", 

{%- for len in lens %}

sum(case when len = '{{len}}' then revenue end) 

as TERM_{{dbt_utils.slugify(len)}}

{%- if not loop.last %},{% endif -%}

{% endfor %}

from group_by_step3

group by 1, 2