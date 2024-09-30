
  
  create view "clinical_trials"."main"."raw_clinicaltrials_studies__dbt_tmp" as (
    -- models/raw_data/raw_clinicaltrials_studies.sql
-- Query the raw clinical trials data using the source reference
SELECT * FROM "clinical_trials"."raw_data"."raw_clinicaltrials_studies"
  );
