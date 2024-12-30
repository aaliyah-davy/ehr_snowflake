use role sysadmin;
use warehouse compute_wh;
use schema ehr.clean;

-- select
--     meta['data_version']::text as data_version,
--     meta['created']::text as created,
--     meta['revision']::text as revision
-- from
--     ehr.raw.patient_raw_tbl;

----------------------------------------------------------------------------------------------

-- PATIENT INFO TABLE
create or replace view ehr.clean.patient_view as
select 
    -- patient demographics
    patient.value:resource:id::text as patient_id,
    patient.value:resource:resourceType::text as resource_type,
    patient.value:resource:gender::text as gender,
    patient.value:resource:birthDate::date as DOB,
    datediff(year, DOB, current_date) as age,
    patient.value:resource:extension[0]:valueCodeableConcept:coding[0]:display::text as race,
    patient.value:resource:extension[1]:valueCodeableConcept:coding[0]:display::text as ethnicity,
    patient.value:resource:maritalStatus:coding[0]:code::text as marital_status,
    patient.value:resource:multipleBirthBoolean::boolean as multiple_births,

    -- patient geolocation
    patient.value:resource:address[0]:city::text as city,
    patient.value:resource:address[0]:state::text as state,
    patient.value:resource:address[0]:postalCode::text as postal_code,
    patient.value:resource:address[0]:extension[0]:extension[0]:valueDecimal::float as latitude,
    patient.value:resource:address[0]:extension[0]:extension[1]:valueDecimal::float as longitude,
    to_geography('POINT(' || longitude || ' ' || latitude || ')') as coords,

    -- file metadata
    patient.value:resource:identifier[0]:value::text as file_name
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) patient
where patient.value:resource:resourceType = 'Patient';

----------------------------------------------------------------------------------------------


-- ENCOUNTER INFO TABLE
create or replace view ehr.clean.encounter_view as
select 
    -- declare parent as patient table
    encounter.value:resource:patient:reference::text as patient_id,
    uuid_string() as encounter_id,

    -- encounter details
    encounter.value:resource:resourceType::text as resource_type,
    encounter.value:resource:type[0]:text::text as encounter_type,
    encounter.value:resource:status::text as status,  

    -- datetime
    encounter.value:resource:period:start::timestamp_ntz as start_date,
    encounter.value:resource:period:end::timestamp_ntz as end_date,
    timestampdiff(second, start_date, end_date) as duration_sec,

    -- event metadata
    encounter.value:resource:fullUrl::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) encounter
where encounter.value:resource:resourceType = 'Encounter';
        
----------------------------------------------------------------------------------------------

-- CONDITION INFO TABLE
create or replace view ehr.clean.condition_view as
select 
    -- declare parent as patient table
    condition.value:resource:subject:reference::text as patient_id,
    uuid_string() as condition_id,

    -- condition details
    condition.value:resource:resourceType::text as resource_type,
    condition.value:resource:code:coding[0]:display::text as condition_type,
    condition.value:resource:clinicalStatus::text as status,
    condition.value:resource:verificationStatus::text as verification_status,
    
    -- datetime
    condition.value:resource:onsetDateTime::timestamp_ntz as onset_date,
    datediff(year, onset_date, current_date) as years_since_onset,

    -- event metadata
    condition.value:fullUrl::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) condition
where condition.value:resource:resourceType = 'Condition';

----------------------------------------------------------------------------------------------

-- IMMUNIZATION INFO TABLE
create or replace view ehr.clean.immunization_view as
select 
    -- declare parent as patient table
    immunization.value:resource:patient:reference::text as patient_id,
    uuid_string() as immunization_id,

    -- vaccination details
    immunization.value:resource:resourceType::text as resource_type,
    immunization.value:resource:vaccineCode:coding[0]:display::text as vaccine_type,
    immunization.value:resource:status::text as status,
    
    immunization.value:resource:wasNotGiven::boolean as not_given_vaccine,
    -- is this record the primary source of vaccine documentation?
    immunization.value:resource:primarySource::boolean as primary_source, 
    
    -- datetime
    immunization.value:resource:date::timestamp_ntz as vaccine_date,
    datediff(year, vaccine_date, current_date) as years_since_vaccine,

    -- event metadata
    immunization.value:resource:encounter:reference::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) immunization
where immunization.value:resource:resourceType = 'Immunization';

----------------------------------------------------------------------------------------------

-- CARE_PLAN INFO TABLE
create or replace view ehr.clean.care_plan_view as
select 
    -- declare parent as patient table
    care_plan.value:resource:subject:reference::text as patient_id,
    uuid_string() as care_plan_id,

    -- care_plan details
    care_plan.value:resource:resourceType::text as resource_type,
    care_plan.value:resource:category[0]:coding[0]:display::text as care_plan_type,
    care_plan.value:resource:status::text as status,
    care_plan.value:resource:activity::array as details,
    
    -- datetime
    care_plan.value:resource:period:start::date as date_start, 

    -- event metadata
    care_plan.value:resource:context:reference::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) care_plan
where care_plan.value:resource:resourceType = 'CarePlan';

----------------------------------------------------------------------------------------------

-- OBSERVATION INFO TABLE
create or replace view ehr.clean.observation_view as
select 
    -- declare parent as patient table
    observation.value:resource:subject:reference::text as patient_id,
    uuid_string() as observation_id,

    -- observation details
    observation.value:resource:resourceType::text as resource_type,
    observation.value:resource:code:coding[0]:display::text as observation_type,
    observation.value:resource:status::text as status,
    
    observation.value:resource:valueQuantity:value::float as value,
    observation.value:resource:valueQuantity:unit::text as unit,

    -- datetime
    observation.value:resource:effectiveDateTime::timestamp_ntz as date_effective,

    -- event metadata
    observation.value:resource:encounter:reference::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) observation
where observation.value:resource:resourceType = 'Observation';

----------------------------------------------------------------------------------------------

-- DIAGNOSTIC_REPORT INFO TABLE
create or replace view ehr.clean.diagnostic_view as
select 
    -- declare parent as patient table
    diagnostic.value:resource:subject:reference::text as patient_id,
    uuid_string() as diagnostic_id,

    -- diagnostic_report details
    diagnostic.value:resource:resourceType::text as resource_type,
    diagnostic.value:resource:code:coding[0]:display::text as diagnostic_type,
    diagnostic.value:resource:status::text as status,
    
    diagnostic.value:resource:result::array as details, 
    diagnostic.value:resource:performer[0]:display::text as performed_by,
    
    -- datetime
    diagnostic.value:resource:effectiveDateTime::timestamp_ntz as date_effective,
    diagnostic.value:resource:issued::timestamp_ntz as date_issued,
    datediff(year, date_issued, current_date) as years_since_diagnosis,

    -- event metadata
    diagnostic.value:resource:encounter:reference::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) diagnostic
where diagnostic.value:resource:resourceType = 'DiagnosticReport';
----------------------------------------------------------------------------------------------

-- PROCEDURES INFO TABLE
create or replace view ehr.clean.procedure_view as
select 
    -- declare parent as patient table
    procedure.value:resource:subject:reference::text as patient_id,
    uuid_string() as procedure_id,

    -- procedure details
    procedure.value:resource:resourceType::text as resource_type,
    procedure.value:resource:code:coding[0]:display::text as procedure_type,
    procedure.value:resource:status::text as status,
      
    -- datetime
    procedure.value:resource:performedDateTime::timestamp_ntz as date_performed,

    -- event metadata
    procedure.value:resource:encounter:reference::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) procedure
where procedure.value:resource:resourceType = 'Procedure';
----------------------------------------------------------------------------------------------

-- MEDICATIONS_REQUEST (MED_REQ) INFO TABLE
create or replace view ehr.clean.med_req_view as
select 
    -- declare parent as patient table
    med_req.value:resource:patient:reference::text as patient_id,
    uuid_string() as med_req_id,

    -- med_req details
    med_req.value:resource:resourceType::text as resource_type,
    med_req.value:resource:stage:coding[0]:code::text as med_req_type,
    med_req.value:resource:status::text as status,
    
    med_req.value:resource:medicationCodeableConcept:coding[0]:display::text as medication,
    med_req.value:resource:reasonReference[0]:reference::text as reason,
    med_req.value:resource:dosageInstructions::array as details,
    
    -- datetime
    med_req.value:resource:dateWritten::timestamp_ntz as date_written,

    -- event metadata
    med_req.value:resource:context:reference::text as event_tracker
    
from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) med_req
where med_req.value:resource:resourceType = 'MedicationRequest';
----------------------------------------------------------------------------------------------

-- ALLERGY_INTOLERANCE INFO TABLE
create or replace view ehr.clean.intolerance_view as
select 
    -- declare parent as patient table
    intolerance.value:resource:patient:reference::text as patient_id,
    uuid_string() as intolerance_id,

    -- intolerance details
    intolerance.value:resource:resourceType::text as resource_type,
    intolerance.value:resource:type::text as intolerance_type,
    intolerance.value:resource:clinicalStatus::text as status,
    intolerance.value:resource:criticality::text as criticality, 
    
    intolerance.value:resource:category[0]::text as category,
    intolerance.value:resource:code:coding[0]:display::text as details,
    
    -- datetime
    intolerance.value:resource:assertedDate::timestamp_ntz as date_asserted

from
    ehr.raw.patient_raw_tbl tbl,
    lateral flatten (input => tbl.entry) intolerance
where intolerance.value:resource:resourceType = 'AllergyIntolerance';
----------------------------------------------------------------------------------------------

