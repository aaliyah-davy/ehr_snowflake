use role sysadmin;
use warehouse compute_wh;
use schema ehr.load;

----- DIMENSION TABLES -----

--- PATIENT ---
merge into ehr.dim.patient as target
using (
    select
    -- patient demographics
    patient_id,
    gender,
    DOB,
    age,
    race,
    ethnicity,
    marital_status,
    multiple_births,

    -- patient geolocation
    city,
    state,
    postal_code,
    coords,

    -- file metadata
    file_name
    from ehr.clean.patient_view
) as source
on target.patient_id = source.patient_id
when matched then update set
    -- patient demographics
    target.patient_id = source.patient_id,
    target.gender = source.gender,
    target.DOB = source.DOB,
    target.age = source.age,
    target.race = source.race,
    target.ethnicity = source.ethnicity,
    target.marital_status = source.marital_status,
    target.multiple_births = source.multiple_births,

    -- patient geolocation
    target.city = source.city,
    target.state = source.state,
    target.postal_code = source.postal_code,
    target.coords = source.coords,

    -- file metadata
    target.file_name = source.file_name
when not matched then insert (
    patient_id, gender, DOB, age, race, ethnicity, marital_status, multiple_births, city, state, postal_code, coords, file_name
) values (
    source.patient_id, source.gender, source.DOB, source.age, source.race, source.ethnicity, source.marital_status, source.multiple_births, source.city, source.state, source.postal_code, source.coords, source.file_name
);

--- CONDITIONS ---
merge into ehr.dim.condition as target
using (
    select
    condition_id,
    patient_id,
    event_tracker,
    
    status,
    verification_status
    from ehr.clean.condition_view
) as source
on target.condition_id = source.condition_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.condition_id = source.condition_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.status = source.status,
    target.verification_status = source.verification_status
when not matched then insert (
    condition_id, patient_id, event_tracker, status, verification_status
) values (
    source.condition_id, source.patient_id, source.event_tracker, source.status, source.verification_status
);

--- IMMUNIZATION ---
merge into ehr.dim.immunization as target
using (
    select
        immunization_id,
        patient_id,
        event_tracker,

        status,
        primary_source
    from ehr.clean.immunization_view
) as source
on target.immunization_id = source.immunization_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.immunization_id = source.immunization_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.status = source.status,
    target.primary_source = source.primary_source
when not matched then insert (
    immunization_id, patient_id, event_tracker, status, primary_source
) values (
    source.immunization_id, source.patient_id, source.event_tracker, source.status, source.primary_source
);

--- CARE_PLAN ---
merge into ehr.dim.care_plan as target
using (
    select
        care_plan_id,
        patient_id,
        event_tracker,
        
        status,
        details
    from ehr.clean.care_plan_view
) as source
on target.care_plan_id = source.care_plan_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.care_plan_id = source.care_plan_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.status = source.status,
    target.details = source.details
when not matched then insert (
    care_plan_id, patient_id, event_tracker, status, details
) values (
    source.care_plan_id, source.patient_id, source.event_tracker, source.status, source.details
);

--- OBSERVATIONS ---
merge into ehr.dim.observation as target
using (
    select
        observation_id,
        patient_id,
        event_tracker,
        
        status,
        unit
    from ehr.clean.observation_view
) as source
on target.observation_id = source.observation_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.observation_id = source.observation_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,

    target.status = source.status,
    target.unit = source.unit
when not matched then insert (
    observation_id, patient_id, event_tracker, status, unit
) values (
    source.observation_id, source.patient_id, source.event_tracker, source.status, source.unit
);

--- DIAGNOSTIC ---
merge into ehr.dim.diagnostic as target
using (
    select
        diagnostic_id,
        patient_id,
        event_tracker,
        
        status,
        details,
        performed_by
    from ehr.clean.diagnostic_view
) as source
on target.diagnostic_id = source.diagnostic_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.diagnostic_id = source.diagnostic_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.status = source.status,
    target.details = source.details,
    target.performed_by = source.performed_by
when not matched then insert (
    diagnostic_id, patient_id, event_tracker, status, details, performed_by
) values (
    source.diagnostic_id, source.patient_id, source.event_tracker, source.status, source.details, source.performed_by
);

--- PROCEDURES ---
merge into ehr.dim.procedure as target
using (
    select
        procedure_id,
        patient_id,
        event_tracker,

        status
    from ehr.clean.procedure_view
) as source
on target.procedure_id = source.procedure_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.procedure_id = source.procedure_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.status = source.status
when not matched then insert (
    procedure_id, patient_id, event_tracker, status
) values (
    source.procedure_id, source.patient_id, source.event_tracker, source.status
);

--- MED_REQUEST ---
merge into ehr.dim.med_req as target
using (
    select
        med_req_id,
        patient_id,
        event_tracker,
        
        reason,
        details
    from ehr.clean.med_req_view
) as source
on target.med_req_id = source.med_req_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.med_req_id = source.med_req_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.reason = source.reason,
    target.details = source.details
when not matched then insert (
    med_req_id, patient_id, event_tracker, reason, details
) values (
    source.med_req_id, source.patient_id, source.event_tracker, source.reason, source.details
);

--- INTOLERANCE ---
merge into ehr.dim.intolerance as target
using (
    select
        intolerance_id,
        patient_id,

        status,
        criticality,
        category,
        details
    from ehr.clean.intolerance_view
) as source
on target.intolerance_id = source.intolerance_id
and target.patient_id = source.patient_id
when matched then update set
    target.intolerance_id = source.intolerance_id,
    target.patient_id = source.patient_id,

    target.status = source.status,
    target.criticality = source.criticality,
    target.category = source.category,
    target.details = source.details
when not matched then insert (
    intolerance_id, patient_id, status, criticality, category, details
) values (
    source.intolerance_id, source.patient_id, source.status, source.criticality, source.category, source.details
);

----------------------------------------------------------------------------------------------

---- FACT TABLES ----

--- ENCOUNTERS ---
merge into ehr.fact.encounter as target
using (
    select
        encounter_id,
        patient_id,
        event_tracker,

        encounter_type,
        status,
        start_date,
        end_date,
        duration_sec
    from ehr.clean.encounter_view
) as source
on target.encounter_id = source.encounter_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.encounter_id = source.encounter_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,

    target.type = source.encounter_type,
    target.status = source.status,
    target.start_date = source.start_date,
    target.end_date = source.end_date,
    target.duration = source.duration_sec
when not matched then insert (
    encounter_id, patient_id, event_tracker, type, status, start_date, end_date, duration
) values (
    source.encounter_id, source.patient_id, source.event_tracker, source.encounter_type, source.status, source.start_date, source.end_date, source.duration_sec
);


--- CONDITIONS ---
merge into ehr.fact.condition as target
using (
    select
        condition_id,
        patient_id,
        
        condition_type,
        onset_date,
        years_since_onset
    from ehr.clean.condition_view
) as source
on target.condition_id = source.condition_id
and target.patient_id = source.patient_id
when matched then update set
    target.condition_id = source.condition_id,
    target.patient_id = source.patient_id,
    
    target.type = source.condition_type,
    target.onset_date = source.onset_date,
    target.years_since_onset = source.years_since_onset
when not matched then insert (
    condition_id, patient_id, type, onset_date, years_since_onset
) values (
    source.condition_id, source.patient_id, source.condition_type, source.onset_date, source.years_since_onset
);

--- IMMUNIZATION ---
merge into ehr.fact.immunization as target
using (
    select
        immunization_id,
        patient_id,
        event_tracker,
        
        vaccine_type,
        not_given_vaccine,
        vaccine_date,
        years_since_vaccine
    from ehr.clean.immunization_view
) as source
on target.immunization_id = source.immunization_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.immunization_id = source.immunization_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.type = source.vaccine_type,
    target.not_given_vaccine = source.not_given_vaccine,
    target.vaccine_date = source.vaccine_date,
    target.years_since_vaccine = source.years_since_vaccine
when not matched then insert (
    immunization_id, patient_id, event_tracker, type, not_given_vaccine, vaccine_date, years_since_vaccine
) values (
    source.immunization_id, source.patient_id, source.event_tracker, source.vaccine_type, source.not_given_vaccine, source.vaccine_date, source.years_since_vaccine
);


--- CARE_PLAN ---
merge into ehr.fact.care_plan as target
using (
    select
        care_plan_id,
        patient_id,
        event_tracker,
        
        care_plan_type,
        date_start
    from ehr.clean.care_plan_view
) as source
on target.care_plan_id = source.care_plan_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.care_plan_id = source.care_plan_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.type = source.care_plan_type,
    target.date_start = source.date_start
when not matched then insert (
    care_plan_id, patient_id, event_tracker, type, date_start
) values (
    source.care_plan_id, source.patient_id, source.event_tracker, source.care_plan_type, source.date_start
);

--- OBSERVATIONS ---
merge into ehr.fact.observation as target
using (
    select
        observation_id,
        patient_id,
        event_tracker,
        
        observation_type,
        value,
        date_effective
    from ehr.clean.observation_view
) as source
on target.observation_id = source.observation_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.observation_id = source.observation_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.type = source.observation_type,
    target.value = source.value,
    target.date_effective = source.date_effective
when not matched then insert (
    observation_id, patient_id, event_tracker, type, value, date_effective
) values (
    source.observation_id, source.patient_id, source.event_tracker, source.observation_type, source.value, source.date_effective
);

--- DIAGNOSTIC ---
merge into ehr.fact.diagnostic as target
using (
    select
        diagnostic_id,
        patient_id,
        event_tracker,
        
        diagnostic_type,
        date_effective,
        date_issued,
        years_since_diagnosis
    from ehr.clean.diagnostic_view
) as source
on target.diagnostic_id = source.diagnostic_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.diagnostic_id = source.diagnostic_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.type = source.diagnostic_type,
    target.date_effective = source.date_effective,
    target.date_issued = source.date_issued,
    target.years_since_diagnosis = source.years_since_diagnosis
when not matched then insert (
    diagnostic_id, patient_id, event_tracker, type, date_effective, date_issued, years_since_diagnosis
) values (
    source.diagnostic_id, source.patient_id, source.event_tracker, source.diagnostic_type, source.date_effective, source.date_issued, source.years_since_diagnosis
);

--- PROCEDURES ---
merge into ehr.fact.procedure as target
using (
    select
        procedure_id,
        patient_id,
        event_tracker,
        
        procedure_type,
        date_performed
    from ehr.clean.procedure_view
) as source
on target.procedure_id = source.procedure_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.procedure_id = source.procedure_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,
    
    target.type = source.procedure_type,
    target.date_performed = source.date_performed
when not matched then insert (
    procedure_id, patient_id, event_tracker, type, date_performed
) values (
    source.procedure_id, source.patient_id, source.event_tracker, source.procedure_type, source.date_performed
);

--- MED_REQUEST ---
merge into ehr.fact.med_req as target
using (
    select
        med_req_id,
        patient_id,
        event_tracker,

        medication,
        med_req_type,
        date_written
    from ehr.clean.med_req_view
) as source
on target.med_req_id = source.med_req_id
and target.patient_id = source.patient_id
and target.event_tracker = source.event_tracker
when matched then update set
    target.med_req_id = source.med_req_id,
    target.patient_id = source.patient_id,
    target.event_tracker = source.event_tracker,

    target.medication = source.medication,
    target.type = source.med_req_type,
    target.date_written = source.date_written
when not matched then insert (
    med_req_id, patient_id, event_tracker, medication, type, date_written
) values (
    source.med_req_id, source.patient_id, source.event_tracker, source.medication, source.med_req_type, source.date_written
);

--- INTOLERANCE ---
merge into ehr.fact.intolerance as target
using (
    select
        intolerance_id,
        patient_id,
        
        intolerance_type,
        date_asserted
    from ehr.clean.intolerance_view
) as source
on target.intolerance_id = source.intolerance_id
and target.patient_id = source.patient_id
when matched then update set
    target.intolerance_id = source.intolerance_id,
    target.patient_id = source.patient_id,
    
    target.type = source.intolerance_type,
    target.date_asserted = source.date_asserted
when not matched then insert (
    intolerance_id, patient_id, type, date_asserted
) values (
    source.intolerance_id, source.patient_id, source.intolerance_type, source.date_asserted
);
