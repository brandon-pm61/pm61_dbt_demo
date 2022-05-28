with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_inpatient_discharge_procedures') }}

),

renamed as (

    select
		permanent_facility_id,
		ccs_diagnosis_code,
		ccs_diagnosis_description,
		ccs_procedure_code,
		ccs_procedure_description,
		apr_drg_code,
		apr_drg_description,
		apr_mdc_code,
		apr_mdc_description,
		apr_severity_of_illness_code,
		apr_severity_of_illness_description,
		apr_risk_of_mortality,
		apr_medical_surgical_description,
		payment_typology_1,
		payment_typology_2,
		payment_typology_3,
		attending_provider_license_number,
		operating_provider_license_number,
		other_provider_license_number,
		birth_weight,
		abortion_edit_indicator,
		emergency_department_indicator,
		total_charges,
		total_costs,
		ratio_of_total_costs_to_total_charges

    from source

)

select * from renamed
