{{config(
	catalog = "snowflake",
	schema = "DESTINATION",
	materialized = "view"
)}}
with inpatient_discharge_procedures as (

    select * from {{ ref('stg_inpatient_discharge_procedures') }}

),

inpatient_discharge_personal as (

    select * from {{ ref('stg_inpatient_discharge_personal') }}

),

inpatient_discharge_facilities as (

    select * from {{ ref('stg_inpatient_discharge_facilities') }}

),

select_step1 as (
  select
    inpatient_discharge_procedures.permanent_facility_id as permanent_facility_id,
    inpatient_discharge_procedures.ccs_diagnosis_description as ccs_diagnosis_description,
    inpatient_discharge_procedures.ccs_procedure_description as ccs_procedure_description,
    inpatient_discharge_procedures.apr_drg_description as apr_drg_description,
    inpatient_discharge_procedures.apr_mdc_description as apr_mdc_description,
    inpatient_discharge_procedures.apr_severity_of_illness_description as apr_severity_of_illness_description,
    inpatient_discharge_procedures.apr_risk_of_mortality as apr_risk_of_mortality,
    inpatient_discharge_procedures.apr_medical_surgical_description as apr_medical_surgical_description,
    inpatient_discharge_procedures.payment_typology_1 as payment_typology_1,
    inpatient_discharge_procedures.total_charges as total_charges,
    inpatient_discharge_procedures.total_costs as total_costs
  from
    inpatient_discharge_procedures
),
join_step2 as (
  select
    select_step1.permanent_facility_id as permanent_facility_id,
    select_step1.ccs_diagnosis_description as ccs_diagnosis_description,
    select_step1.ccs_procedure_description as ccs_procedure_description,
    select_step1.apr_drg_description as apr_drg_description,
    select_step1.apr_mdc_description as apr_mdc_description,
    select_step1.apr_severity_of_illness_description as apr_severity_of_illness_description,
    select_step1.apr_risk_of_mortality as apr_risk_of_mortality,
    select_step1.apr_medical_surgical_description as apr_medical_surgical_description,
    select_step1.payment_typology_1 as payment_typology_1,
    select_step1.total_charges as total_charges,
    select_step1.total_costs as total_costs,
    inpatient_discharge_personal.age_group as age_group,
    inpatient_discharge_personal.zip_code_3_digits as zip_code_3_digits,
    inpatient_discharge_personal.gender as gender,
    inpatient_discharge_personal.race as race,
    inpatient_discharge_personal.ethnicity as ethnicity,
    inpatient_discharge_personal.length_of_stay as length_of_stay,
    inpatient_discharge_personal.type_of_admission as type_of_admission,
    inpatient_discharge_personal.patient_disposition as patient_disposition,
    inpatient_discharge_personal.discharge_year as discharge_year
  from
    select_step1
    left outer join inpatient_discharge_personal on (
      select_step1.permanent_facility_id = inpatient_discharge_personal.permanent_facility_id
    )
),
join_step3 as (
  select
    join_step2.permanent_facility_id as permanent_facility_id,
    join_step2.ccs_diagnosis_description as ccs_diagnosis_description,
    join_step2.ccs_procedure_description as ccs_procedure_description,
    join_step2.apr_drg_description as apr_drg_description,
    join_step2.apr_mdc_description as apr_mdc_description,
    join_step2.apr_severity_of_illness_description as apr_severity_of_illness_description,
    join_step2.apr_risk_of_mortality as apr_risk_of_mortality,
    join_step2.apr_medical_surgical_description as apr_medical_surgical_description,
    join_step2.payment_typology_1 as payment_typology_1,
    join_step2.total_charges as total_charges,
    join_step2.total_costs as total_costs,
    join_step2.age_group as age_group,
    join_step2.zip_code_3_digits as zip_code_3_digits,
    join_step2.gender as gender,
    join_step2.race as race,
    join_step2.ethnicity as ethnicity,
    join_step2.length_of_stay as length_of_stay,
    join_step2.type_of_admission as type_of_admission,
    join_step2.patient_disposition as patient_disposition,
    join_step2.discharge_year as discharge_year,
    inpatient_discharge_facilities.hospital_service_area as hospital_service_area,
    inpatient_discharge_facilities.hospital_county as hospital_county,
    inpatient_discharge_facilities.operating_certificate_number as operating_certificate_number,
    inpatient_discharge_facilities.facility_name as facility_name
  from
    join_step2
    left outer join inpatient_discharge_facilities on (
      join_step2.permanent_facility_id = inpatient_discharge_facilities.permanent_facility_id
    )
)
select
  *
from
  join_step3
limit
  100
