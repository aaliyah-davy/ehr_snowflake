use role sysadmin;
use warehouse compute_wh;


use schema ehr.dim;

----- DIMENSION TABLES -----

-- PATIENT DIM TABLE: Contains demographic and geographic details about patients.
create or replace table ehr.dim.patient ( 
    -- patient demographics
    patient_id varchar(100) primary key,
    gender varchar(10),
    DOB date,
    age int,
    race varchar(50),
    ethnicity varchar(50),
    marital_status varchar(20),
    multiple_births boolean,

    -- patient geolocation
    city varchar(50),
    state varchar(50),
    postal_code varchar(10),
    coords geography,

    -- file metadata
    file_name varchar(100)
);


-- CONDITIONS DIM TABLE: Describes condition types.
create or replace table ehr.dim.condition (
    -- tracking/cross-ref cols
    condition_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),

    -- condition details
    status varchar(50),
    verification_status varchar(50)
);


-- IMMUNIZATION DIM TABLE: Contains vaccine-specific attributes.
create or replace table ehr.dim.immunization ( 
    -- tracking/cross-ref cols
    immunization_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- vaccination details
    status varchar(50),
    primary_source boolean
);


-- CARE_PLAN DIM TABLE: Contains patient care plans following encounter/observation.
create or replace table ehr.dim.care_plan ( 
    -- tracking/cross-ref cols
    care_plan_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),

    -- care_plan details
    status varchar(50),
    details array
);


-- OBSERVATIONS DIM TABLE: Details about observation types and their statuses.
create or replace table ehr.dim.observation ( 
    -- tracking/cross-ref cols
    observation_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),

    -- observation details
    status varchar(50),
    unit varchar(50)
);

-- DIAGNOSTIC REPORT DIM TABLE: Conatins details of patient diagnostic reports.
create or replace table ehr.dim.diagnostic ( 
    -- tracking/cross-ref cols
    diagnostic_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- diagnostic details
    status varchar(50),
    details array, 
    performed_by varchar(50)
);


-- PROCEDURES DIM TABLE: Describes various types of procedures.
create or replace table ehr.dim.procedure ( 
    -- tracking/cross-ref cols
    procedure_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- procedure details
    status varchar(50)
);


-- MED_REQUEST DIM TABLE: Describes medication-related information.
create or replace table ehr.dim.med_req ( 
    -- tracking/cross-ref cols
    med_req_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- med_req details
    reason varchar(100),
    details array
);


-- INTOLERANCE DIM TABLE: Describes patient allergy intolerance.
create or replace table ehr.dim.intolerance ( 
    -- tracking/cross-ref cols
    intolerance_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    
    -- intolerance details
    status varchar(50),
    criticality varchar(50),
    category varchar(50),
    details varchar(100)
);

----------------------------------------------------------------------------------------------

use schema ehr.fact;

---- FACT TABLES ----

-- ENCOUNTER FACT TABLE: Captures encounter details for patients.
create or replace table ehr.fact.encounter ( 
    -- tracking/cross-ref cols
    encounter_id varchar(36) primary key,
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- encounter details
    type varchar(100),
    status varchar(50),
    start_date timestamp_ntz,
    end_date timestamp_ntz,
    duration bigint
);


-- CONDITIONS FACT TABLE: Stores details of patient conditions.
create or replace table ehr.fact.condition ( 
    -- tracking/cross-ref cols
    condition_id varchar(36) references ehr.dim.condition(condition_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),

    -- condition details
    type varchar(100),
    onset_date timestamp_ntz,
    years_since_onset int
);


-- IMMUNIZATION FACT TABLE: Tracks vaccination events.
create or replace table ehr.fact.immunization ( 
    -- tracking/cross-ref cols
    immunization_id varchar(36) references ehr.dim.immunization(immunization_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- vaccination details
    type varchar(100),
    not_given_vaccine boolean,
    vaccine_date timestamp_ntz,
    years_since_vaccine int
);


-- CARE_PLAN FACT TABLE: Conatins patient care plan data.
create or replace table ehr.fact.care_plan ( 
    -- tracking/cross-ref cols
    care_plan_id varchar(36) references ehr.dim.care_plan(care_plan_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- care_plan details
    type varchar(100),
    date_start date
);


-- OBSERVATIONS FACT TABLE: Holds observation data for patients.
create or replace table ehr.fact.observation ( 
    -- tracking/cross-ref cols
    observation_id varchar(36) references ehr.dim.observation(observation_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- observation details
    type varchar(100),
    value float,
    date_effective timestamp_ntz
);


-- DIAGNOSTIC REPORT FACT TABLE: Contains patient dognostic data.
create or replace table ehr.fact.diagnostic ( 
    -- tracking/cross-ref cols
    diagnostic_id varchar(36) references ehr.dim.diagnostic(diagnostic_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- diagnostic details
    type varchar(100),
    date_effective timestamp_ntz,
    date_issued timestamp_ntz,
    years_since_diagnosis int
);


-- PROCEDURES FACT TABLE: Holds various patient procedure data.
create or replace table ehr.fact.procedure ( 
    -- tracking/cross-ref cols
    procedure_id varchar(36) references ehr.dim.procedure(procedure_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- procedure details
    type varchar(100),
    date_performed timestamp_ntz
);


-- MED_REQUEST FACT TABLE: Logs medication requests made for patients.
create or replace table ehr.fact.med_req ( 
    -- tracking/cross-ref cols
    med_req_id varchar(36) references ehr.dim.med_req(med_req_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    event_tracker varchar(100),
    
    -- med_req details
    type varchar(100),
    medication varchar(100),
    date_written timestamp_ntz
);


-- INTOLERANCE FACT TABLE: Conatins info on patient allergy intolerance.
create or replace table ehr.fact.intolerance ( 
    -- tracking/cross-ref cols
    intolerance_id varchar(36) references ehr.dim.intolerance(intolerance_id),
    patient_id varchar(100) references ehr.dim.patient(patient_id),
    
    -- intolerance details
    type varchar(100),
    date_asserted timestamp_ntz
);

