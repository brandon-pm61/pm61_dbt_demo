with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_inpatient_discharge_facilities') }}

),

renamed as (

    select
        hospital_service_area, 
        hospital_county, 
        operating_certificate_number, 
        permanent_facility_id, 
        facility_name

    from source

)

select * from renamed
