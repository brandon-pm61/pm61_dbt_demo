with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_inpatient_discharge_personal') }}

),

renamed as (

    select
        permanent_facility_id, 
        age_group, 
        zip_code_3_digits, 
        gender, 
        race, 
        ethnicity, 
        length_of_stay, 
        type_of_admission, 
        patient_disposition, 
        discharge_year

    from source

)

select * from renamed
