WITH source as (
    SELECT * FROM {{ source('kumon_raw', 'dim_students') }}
),

renamed as (
    SELECT
        -- IDs and keys
        cast(student_id as string) as student_unique_id
        , cast(kumon_id as string) as business_key
        
        -- Student attributes
        , cast(name as string) as student_name
        , cast(birth_date as date) as birth_date
        , cast(gender as string) as gender

        -- Enrollment attributes
        , cast(subject as string) as subject_name
        , cast(type as string) as study_type
        , cast(current_grade as string) as current_school_grade
        , cast(current_stage as string) as current_kumon_stage
        , cast(enroll_date_sub as date) as enrollment_date
        , cast(status as string) as enrollment_status

        -- Metadata
        , cast(ingested_at as timestamp) as loaded_at
    FROM source
)

SELECT * FROM renamed