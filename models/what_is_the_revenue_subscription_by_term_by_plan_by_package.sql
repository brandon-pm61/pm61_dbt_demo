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
    INNER JOIN "postgresql"."sample_qa"."fact_subscription_activity" ON (
      select_step1."PLAN_ID" = "postgresql"."sample_qa"."fact_subscription_activity"."PLAN_ID"
    )
),
group_by_step3 as (
  SELECT
    join_step2."LEN" AS "TERM",
    join_step2."PLAN_NAME" AS "PLAN_NAME",
    join_step2."PACKAGE" AS "PACKAGE",
    sum(MER) as "REVENUE"
  FROM
    join_step2
  GROUP BY
    join_step2."LEN",
    join_step2."PLAN_NAME",
    join_step2."PACKAGE"
)
SELECT
  *
FROM
  group_by_step3
LIMIT
  100