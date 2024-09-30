WITH base_studies AS (
    SELECT 
        s.nct_id,
        s.protocol_section__identification_module__brief_title as brief_title,
        s.protocol_section__identification_module__official_title as official_title,
        s.protocol_section__status_module__overall_status as overall_status,
        s.protocol_section__status_module__start_date_struct__date as start_date,
        s.protocol_section__status_module__completion_date_struct__date as completion_date,
        s.protocol_section__design_module__study_type as study_type,
        s.protocol_section__design_module__enrollment_info__type as enrollment,
        STRING_AGG(DISTINCT e.value, ', ') AS condition,
        STRING_AGG(DISTINCT sc.name, ', ') AS sponsor_name,
        STRING_AGG(DISTINCT i.name, ', ') AS investigator_name,
        STRING_AGG(DISTINCT ae.serious_num_affected::TEXT, ', ') AS serious_adverse_events,
        STRING_AGG(DISTINCT bm.title, ', ') AS baseline_measure,
        STRING_AGG(DISTINCT om.title, ', ') AS outcome_measure
    FROM {{ ref('raw_clinicaltrials_studies') }} s
    LEFT JOIN {{ source('raw_data', 'raw_clinicaltrials_studies__protocol_section__conditions_module__conditions') }} e ON s._dlt_id = e._dlt_parent_id
    LEFT JOIN {{ source('raw_data', 'raw_clinicaltrials_studies__protocol_section__sponsor_collaborators_module__collaborators') }} sc ON s._dlt_id = sc._dlt_parent_id
    LEFT JOIN {{ source('raw_data', 'raw_clinicaltrials_studies__protocol_section__arms_interventions_module__interventions') }} i ON s._dlt_id = i._dlt_parent_id
    LEFT JOIN {{ source('raw_data', 'raw_clinicaltrials_studies__results_section__adverse_events_module__event_groups') }} ae ON s._dlt_id = ae._dlt_parent_id
    LEFT JOIN {{ source('raw_data', 'raw_clinicaltrials_studies__results_section__baseline_characteristics_module__measures') }} bm ON s._dlt_id = bm._dlt_parent_id
    LEFT JOIN {{ source('raw_data', 'raw_clinicaltrials_studies__results_section__outcome_measures_module__outcome_measures') }} om ON s._dlt_id = om._dlt_parent_id
    GROUP BY s.nct_id, brief_title, official_title, overall_status, start_date, completion_date, study_type, enrollment
)
SELECT * FROM base_studies

