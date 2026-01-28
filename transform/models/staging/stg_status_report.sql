WITH source as (
    SELECT * FROM {{ source('kumon_raw', 'fct_status_report') }}
),

renamed as (
    SELECT
        -- IDs and keys
        cast(fact_id as string) as fact_id
        , cast(student_id as string) as student_unique_id
        
        -- Report context
        , cast(report_date as date) as report_date
        , cast(subject as string) as subject_name
        , cast(type as string) as study_type
        , cast(grade as string) as grade_at_report_date
        , cast(stage as string) as stage_at_report_date

        -- Performance metrics
        , cast(current_lesson as integer) as lesson_at_report_date
        , cast(total_sheets as integer) as worksheets_completed

        -- Student status
        , cast(advanced as integer) as is_advanced
        , cast(status as string) as enrollment_status

        -- Metadata
        , cast(ingested_at as timestamp) as loaded_at

    FROM source
)

SELECT * FROM renamed