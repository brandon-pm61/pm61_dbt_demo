{{config(
	catalog = "snowflake",
	schema = "DESTINATION",
	materialized = "view"
)}}
SELECT
  "FACILITY_NAME", "DISCHARGE_YEAR", "ETHNICITY", "AGE_GROUP",
  AVG("LENGTH_OF_STAY") AVG_STAY, SUM("TOTAL_CHARGES") TOTAL_CHARGE, SUM("TOTAL_COSTS") TOTAL_COST
FROM
  {{ ref('inpatient_discharge_info') }}
GROUP by
  "FACILITY_NAME", "DISCHARGE_YEAR", "ETHNICITY", "AGE_GROUP"