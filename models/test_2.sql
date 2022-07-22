WITH select_step1 as (
  SELECT
    "raw_inpatient_discharge_procedures"."PERMANENT_FACILITY_ID" AS "PERMANENT_FACILITY_ID",
    "raw_inpatient_discharge_procedures"."CCS_DIAGNOSIS_CODE" AS "CCS_DIAGNOSIS_CODE",
    "raw_inpatient_discharge_procedures"."CCS_DIAGNOSIS_DESCRIPTION" AS "CCS_DIAGNOSIS_DESCRIPTION"
  FROM
    "DESTINATION"."raw_inpatient_discharge_procedures"
)
SELECT
  *
FROM
  select_step1
LIMIT
  100